<?php

namespace App\Consumer;

use App\Consumer\Exception\ConsumerException;
use App\Events\BaseRecord;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Metadata;
use Throwable;

class Consumer
{

    use RecordFormatter;
    use ShortClassName;

    /** @var KafkaSerializerInterface */
    private $serializer;

    /** @var KafkaConsumer */
    private $kafkaClient;

    /** @var LoggerInterface */
    private $logger;

    /** @var RecordProcessor */
    private $recordProcessor;

    public function __construct(
      KafkaConsumer $kafkaClient,
      //      KafkaSerializerInterface $serializer,
      LoggerInterface $logger,
      RecordProcessor $recordProcessor
    ) {
        $this->kafkaClient = $kafkaClient;
        //        $this->serializer = $serializer;
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

        $this->subscribeToTopics($topics);

        $this->poll();
    }

    private function subscribeToTopics(array $topics)
    {
        try {
            $this->kafkaClient->subscribe($topics);
        } catch (Throwable $e) {
            $this->logger->log(
              Logger::ERROR,
              sprintf('Error subscribing to topics %s: %s ', implode(',', $topics), $e->getMessage())
            );
            throw $e;
        }
    }

    private function poll(): void
    {
        try {
            while (true) {
                // todo -- what is a good timeout here?
                $message = $this->kafkaClient->consume(1000);
                if ($message) {
                    $this->recordProcessor->process($message);
                }
            }
        } catch (Throwable $throwable) {
            $this->logger->error($throwable->getMessage());
            throw $throwable;
        } finally {
            $this->kafkaClient->unsubscribe();
        }
    }

    private function determineTopics($topics = []): array
    {
        if (!is_array($topics)) {
            $topics = [$topics];
        }

        return count($topics) > 0 ? $topics : $this->determineDefaultTopics();
    }
    
    public function getMetadata(bool $all_topics, ?KafkaConsumerTopic $only_topic, int $timeout_ms): Metadata
    {
        return $this->kafkaClient->getMetadata($all_topics, $only_topic, $timeout_ms);
    }

    public function determineDefaultTopics(): array
    {
        $handlers = $this->recordProcessor->getHandlers();
        if (count($handlers) < 1) {
            throw new ConsumerException('Unable to determine default topics because there are no subscriptions registered.');
        }

        return array_keys($handlers);
    }
}