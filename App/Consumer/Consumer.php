<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Exception;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use FlixTech\SchemaRegistryApi\Registry;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Message;
use RdKafka\Metadata;
use Throwable;

class Consumer
{

    use RecordFormatter;
    use ShortClassName;

    /** @var \App\Serializers\KafkaSerializerInterface */
    private $serializer;

    /** @var \RdKafka\KafkaConsumer */
    private $kafkaClient;

    /** @var \Psr\Log\LoggerInterface */
    private $logger;

    /** @var \App\Consumer\RecordProcessor */
    private $recordProcessor;

    public function __construct(
      KafkaConsumer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger,
      Registry $registry,
      RecordProcessor $recordProcessor = null
    ) {
        $this->kafkaClient = $kafkaClient;
        $this->serializer = $serializer;
        $this->logger = $logger;
        $this->registry = $registry;
        $this->recordProcessor = $recordProcessor ?? new RecordProcessor($this->registry, $this->serializer);
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
     */
    public function consume($topics = []): void
    {
        // todo -- handle when string
        $topics = count($topics) > 0 ? $topics : $this->determineDefaultTopics();

        try {
            $this->kafkaClient->subscribe($topics);
        } catch (Throwable $e) {
            $this->logger->log(
              Logger::ERROR,
              sprintf('Error subscribing to topics %s: %s ', implode(',', $topics), $e->getMessage())
            );
        }

        try {
            while (true) {
                // todo -- what is a good timeout here?
                $message = $this->kafkaClient->consume(1000);

                // If there are no new messages for Kafka to grab, the Message object return is empty save for a -185
                // error code indicating a timeout.
                if (!$message->err) {
                    $this->recordProcessor->process($message);
                    $this->kafkaClient->commit($message);
                } else {
                    print $message->err;
                }
            }
        } catch (Throwable $throwable) {
            var_dump($throwable);
            // log & throw
        } finally {
            $this->kafkaClient->unsubscribe();
        }
    }


    private function handleMessage(Message $message)
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $this->handleSuccess($message);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return $this->handlePartitionEof();
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->handleTimeOut();
                break;
            default:
                $this->handleError($message);
                break;
        }
    }

    private function handleSuccess(Message $message, BaseRecord $record)
    {
        try {
            print 'no error';
            $decoded = $this->serializer->deserialize($message->payload, $record);
            try {
                return ($this->successCallback)($decoded);
            } catch (Throwable $t) {
                print 'caught error';
                $this->logger->log(
                  Logger::ERROR,
                  sprintf('Error executing success callback: ' . $t->getMessage())
                );
            }
        } catch (AvroDecodingException $e) {
            //            $this->logAvroDecodingException($e);
        } catch (Throwable $t) {
            $this->logger->log(Logger::ERROR, $t->getMessage());
        }

    }


    private function handlePartitionEof(): string
    {
        return 'No more messages; will wait for more';
    }

    private function handleTimeOut(): string
    {
        return 'Time out';
    }

    private function handleError(Message $message, BaseRecord $record)
    {
        if ($this->errorCallback) {
            return ($this->errorCallback)();
        }
        $msg = sprintf('%s for record %s: %s', $message->errstr(), $record->name(), $message->err);
        $this->logger->log(Logger::ERROR, $msg);
        throw new KafkaException($msg);
    }

    public function onError(callable $callback): void
    {
        $this->errorCallback = $callback;
    }

    public function onSuccess(callable $callback): void
    {
        $this->successCallback = $callback;
    }

    public function getTopics(): array
    {
        return $this->topics;
    }

    public function getAssignment(): array
    {
        return $this->kafkaClient->getAssignment();
    }

    public function getMetadata(bool $all_topics, ?KafkaConsumerTopic $only_topic, int $timeout_ms): Metadata
    {
        return $this->kafkaClient->getMetadata($all_topics, $only_topic, $timeout_ms);
    }

    public function getSubscription(): array
    {
        return $this->kafkaClient->getSubscription();
    }

    public function determineDefaultTopics()
    {
        $handlers = $this->recordProcessor->getHandlers();
        if (count($handlers) < 1) {
            // todo -- use ConsumerException class from last iteration.
            throw new Exception('Unable to determine default topics because there are no subscriptions registered.');
        }

        return array_keys($handlers);

    }
}