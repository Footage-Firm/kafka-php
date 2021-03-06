<?php

namespace KafkaPhp\Consumer;

use KafkaPhp\Consumer\Exceptions\ConsumerConfigurationException;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use KafkaPhp\Traits\RecordFormatter;
use KafkaPhp\Traits\ShortClassName;
use Carbon\Carbon;
use Psr\Log\LoggerInterface;
use RdKafka\Exception;
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

        //TODO: We were misusing async by not using Tasks to bootstrap each sub-task. Refactor to support this... for now just force sync.
        Pool::$forceSynchronous = true;
        $this->pool = Pool::create();
    }

    public function subscribe(string $record, callable $handler, ?callable $failure = null): self
    {
        $this->recordProcessor->subscribe($record, $handler, $failure);
        return $this;
    }

    /**
     * @param  string[]|string $topics
     * @return Consumer
     * @throws \RdKafka\Exception
     * @throws \Throwable
     */
    public function consume($topics = []): Consumer
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

        return $this;
    }

    public function getMetadata(
      bool $allTopics = true,
      KafkaConsumerTopic $onlyTopic = null,
      int $timeout_ms = 1000
    ): Metadata {
        $onlyTopic = $onlyTopic ?? new KafkaConsumerTopic();
        return $this->kafkaClient->getMetadata($allTopics, $onlyTopic, $timeout_ms);
    }

    public function disconnect(): self
    {
        $this->connected = false;
        return $this;
    }

    public function wait(): void
    {
        $this->pool->wait();
    }

    private function startPolling(): void
    {
        $this->pool->add(function ()
        {
            $this->poll();
        })->catch(function (Throwable $exception)
        {
            throw $exception;
        });
    }

    private function poll(): void
    {

        $this->connected = true;
        $this->lastMessageTime = Carbon::now();

        while ($this->connected && $this->idleTimeRemaining()) {
            try {
                $message = $this->kafkaClient->consume($this->connectTimeoutMs);
            } catch (Throwable $t) {
                $this->logger->error('KafkaConsumer: Error consuming message', ['message' => $t->getMessage()]);
            }

            if (!$message || !is_object($message)) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->logger->info('Processing message.', ['topic' => $message->topic_name, 'partition' => $message->partition, 'offset' => $message->offset]);
                    $record = $this->serializer->deserialize($message->payload);
                    $this->recordProcessor->process($record);
                    try {
                        $this->kafkaClient->commit($message);
                    } catch (Exception $rdkafkaException) {
                        if (preg_match("/Request timed out/", $rdkafkaException->getMessage())) {
                            // retry once on timeout
                            $this->logger->warning("Retrying commit after timeout.", ['topic' => $message->topic_name, 'partition' => $message->partition, 'offset' => $message->offset]);
                            $this->kafkaClient->commit($message);
                        }
                    }
                    $this->lastMessageTime = Carbon::now();
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // NOP. If there are no new messages in the subscribed topic then a message with this error will be
                    // sent after the consume timeout
                    break;
                default:
                    $this->logger->error('Kafka message error.', ['error' => $message->errstr()]);
                    break;
            }

            usleep($this->pollIntervalMs * 1000);
        }
    }

    private function idleTimeRemaining(): bool
    {
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