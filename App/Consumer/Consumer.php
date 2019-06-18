<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use Monolog\Logger;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Message;
use RdKafka\Metadata;
use Throwable;

class Consumer
{

    private $serializer;

    private $config;

    private $errorCallback;

    private $successCallback;

    private $topics;

    private $kafkaConsumer;

    private $logger;

    public function __construct(ConsumerConfig $config)
    {
        $this->config = $config;
        $this->serializer = $config->getSerializer();
        $this->logger = $config->getLogger();
        $this->kafkaConsumer = new KafkaConsumer($this->config);
    }

    public function consume(array $topics, BaseRecord $record): void
    {
        try {
            $this->kafkaConsumer->subscribe($topics);
        } catch (Throwable $e) {
            $this->logger->log(
              Logger::ERROR,
              sprintf('Error subscribing to topics %s: %s ', implode(',', $topics), $e->getMessage())
            );
        }
        try {
            while (true) {
                $message = $this->kafkaConsumer->consume($this->config->getTimeout());
                $this->handleMessage($message, $record);
            }
        } finally {
            $this->kafkaConsumer->unsubscribe();
        }
    }

    private function handleMessage(Message $message, BaseRecord $record)
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $this->handleSuccess($message, $record);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return $this->handlePartitionEof();
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->handleTimeOut();
                break;
            default:
                $this->handleError($message, $record);
                break;
        }
    }

    private function handleSuccess(Message $message, BaseRecord $record)
    {
        try {

            $decoded = $this->serializer->deserialize($message->payload, $record);
            try {
                return ($this->successCallback)($decoded);
            } catch (Throwable $t) {
                $this->logger->log(
                  Logger::ERROR,
                  sprintf('Error executing success callback: ' . $t->getMessage())
                );
            }
        } catch (AvroDecodingException $e) {
            $this->logAvroDecodingException($e);
        } catch (Throwable $t) {
            $this->logger->log(Logger::ERROR, $t->getMessage());
        }

    }

    private function logAvroDecodingException(AvroDecodingException $e): void
    {
        $prev = $e->getPrevious();

        // parse the reader/writer types
        $matches = [];
        preg_match(
          '/Writer\'s schema .*?"name":"(\w+)".*?Reader\'s schema .*?"name":"(\w+)"/',
          $prev->getMessage(),
          $matches
        );
        [$_, $writerType, $readerType] = $matches;

        $msg = ">>> Skipping message. writerType: $writerType, readerType: $readerType" . PHP_EOL;
        echo $msg;
        $this->logger->log(Logger::WARNING, $msg);
    }


    private function handlePartitionEof(): string
    {
        return 'No more messages; will wait for more';
    }

    private function handleTimeOut(): string
    {
        Return 'Time out';
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
        return $this->kafkaConsumer->getAssignment();
    }

    public function getMetadata(bool $all_topics, ?KafkaConsumerTopic $only_topic, int $timeout_ms): Metadata
    {
        return $this->kafkaConsumer->getMetadata($all_topics, $only_topic, $timeout_ms);
    }

    public function getSubscription(): array
    {
        return $this->kafkaConsumer->getSubscription();
    }

}