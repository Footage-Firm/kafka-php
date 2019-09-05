<?php

namespace App\Consumer;

use App\Consumer\Exceptions\ConsumerConfigurationException;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Carbon\Carbon;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Metadata;
use Spatie\Async\Pool;
use Throwable;

class Consumer
{

    use RecordFormatter;
    use ShortClassName;

    /** @var KafkaConsumer */
    private $kafkaClient;

    /** @var LoggerInterface */
    private $logger;

    /** @var RecordProcessor */
    private $recordProcessor;

    /** @var KafkaSerializerInterface */
    private $serializer;

    /** @var int */
    private $connectTimeoutMs;

    /** @var int */
    private $idleTimeoutMs;

    /** @var int */
    private $lastMessageTime;

    /** @var int */
    private $pollIntervalMs;

    /** @var bool */
    private $connected = false;

    /** @var Pool */
    private $pool;

    public function __construct(
      KafkaConsumer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger,
      RecordProcessor $recordProcessor,
      int $idleTimeoutMs = null,
      int $connectTimeoutMs = null,
      int $pollIntervalMs = null
    ) {
        $this->kafkaClient = $kafkaClient;
        $this->serializer = $serializer;
        $this->logger = $logger;
        $this->recordProcessor = $recordProcessor;
        $this->idleTimeoutMs = $idleTimeoutMs;
        $this->connectTimeoutMs = $connectTimeoutMs ?? ConsumerBuilder::DEFAULT_TIMEOUT_MS;
        $this->pollIntervalMs = $pollIntervalMs ?? ConsumerBuilder::DEFAULT_POLL_INTERVAL_MS;

        $this->pool = Pool::create();
    }

    public function subscribe(string $record, callable $handler, ?callable $failure = null): self
    {
        $this->recordProcessor->subscribe($record, $handler, $failure);
        return $this;
    }

    /**
     * @param  string[]|string  $topics
     *
     * @throws \RdKafka\Exception
     * @throws \Throwable
     */
    public function consume($topics = []): void
    {
        $topics = $this->determineTopics($topics);

        try {
            $this->kafkaClient->subscribe($topics);
            $this->startPolling();
        } catch (Throwable $throwable) {
            $this->logger->error($throwable->getMessage());
            throw $throwable;
        } finally {
            $this->kafkaClient->unsubscribe();
        }
    }

    public function getMetadata(bool $all_topics, ?KafkaConsumerTopic $only_topic, int $timeout_ms): Metadata
    {
        return $this->kafkaClient->getMetadata($all_topics, $only_topic, $timeout_ms);
    }

    public function disconnect(): self
    {
        $this->connected = false;
        return $this;
    }

    public function wait(): void {
        $this->pool->wait();
    }

    private function startPolling(): void {
        $this->pool->add(function () {
            $this->poll();
        })->catch(function (Throwable $exception) {
            throw $exception;
        });
    }

    private function poll(): void
    {

        $this->connected = true;
        $this->lastMessageTime = Carbon::now();

        while ($this->connected && $this->idleTimeRemaining()) {
            $message = $this->kafkaClient->consume($this->connectTimeoutMs);

            if (!$message || !is_object($message)) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $record = $this->serializer->deserialize($message->payload);
                    $this->recordProcessor->process($record);
                    $this->kafkaClient->commit($message);
                    $this->lastMessageTime = Carbon::now();
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // NOP. If there are no new messages in the subscribed topic then a message with this error will be
                    // sent after the consume timeout
                    break;
                default:
                    $this->logger->info('Kafka message error: ' . $message->errstr());
                    break;
            }

            usleep($this->pollIntervalMs * 1000);
        }
    }

    private function idleTimeRemaining(): bool {
        $idleMs = Carbon::now()->diffInMilliseconds($this->lastMessageTime);
        return is_numeric($this->idleTimeoutMs) ? $idleMs <= $this->idleTimeoutMs : true;
    }

    private function determineTopics($topics = []): array
    {
        $topics = $topics ?? [];
        $topics = is_array($topics) ? $topics : [$topics];
        return count($topics) > 0 ? $topics : $this->determineDefaultTopics();
    }

    private function determineDefaultTopics(): array
    {
        $handlers = $this->recordProcessor->getHandlers();
        if (count($handlers) < 1) {
            throw new ConsumerConfigurationException('Unable to determine default topics because there are no subscriptions registered.');
        }

        return array_map(function ($className)
        {
            return $this->kebabCase(self::shortClassName($className));
        }, array_keys($handlers));
    }
}