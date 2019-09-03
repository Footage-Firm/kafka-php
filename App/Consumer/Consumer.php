<?php

namespace App\Consumer;

use App\Consumer\Exceptions\ConsumerConfigurationException;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Metadata;
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

    protected $timeout = ConsumerBuilder::DEFAULT_TIMEOUT_MS;

    // The lifetime of the consumer in seconds. Leave null for infinite lifetime.
    private $consumerLifetime;

    public function __construct(
      KafkaConsumer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger,
      RecordProcessor $recordProcessor
    ) {
        $this->kafkaClient = $kafkaClient;
        $this->serializer = $serializer;
        $this->logger = $logger;
        $this->recordProcessor = $recordProcessor;
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
            $this->poll();
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

    public function setConsumerLifetime(int $timeoutSeconds): self
    {
        $this->consumerLifetime = $timeoutSeconds;
        return $this;
    }

    public function setTimeout(int $timeout): self
    {
        $this->timeout = $timeout;
        return $this;
    }

    private function poll(): void
    {
        $time = time();
        while ($this->stillAlive($time)) {

            $message = $this->kafkaClient->consume($this->timeout);

            if (!$message || !is_object($message)) {
                continue;
            }

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $record = $this->serializer->deserialize($message->payload);
                    $this->recordProcessor->process($record);
                    $this->kafkaClient->commit($message);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // NOP. If there are no new messages in the subscribed topic then a message with this error will be
                    // sent after the consume timeout
                    break;
                default:
                    $this->logger->info('Kafka message error: ' . $message->errstr());
                    break;
            }
        }
    }

    private function stillAlive(int $initialTime): bool
    {
        return $this->consumerLifetime === null ? true : time() - $initialTime < $this->consumerLifetime;
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