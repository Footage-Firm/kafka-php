<?php

namespace App\Consumer;

use App\Consumer\Exceptions\ConsumerException;
use App\Events\BaseRecord;
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

    protected $timeout = ConsumerBuilder::DEFAULT_TIMEOUT_MS;

    // The lifetime of the consumer in seconds
    private $consumerLifetime;

    public function __construct(KafkaConsumer $kafkaClient, LoggerInterface $logger, RecordProcessor $recordProcessor)
    {
        $this->kafkaClient = $kafkaClient;
        $this->logger = $logger;
        $this->recordProcessor = $recordProcessor;
    }

    public function subscribe(BaseRecord $record, callable $handler, ?callable $failure = null): self
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
        while (true) {
            if ($this->consumerLifetimeReached($time)) {
                break;
            }

            $message = $this->kafkaClient->consume($this->timeout);
            if ($message) {
                $this->recordProcessor->process($message);
            }
        }
    }

    private function consumerLifetimeReached(int $time): bool
    {
        if ($this->consumerLifetime !== null) {
            return time() - $time > $this->consumerLifetime;
        }

        return false;
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
            throw new ConsumerException('Unable to determine default topics because there are no subscriptions registered.');
        }

        return array_keys($handlers);
    }


}