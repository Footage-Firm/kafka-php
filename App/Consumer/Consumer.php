<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
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

    public function __construct(ConsumerConfig $config)
    {
        $this->config = $config;
        $this->serializer = $config->getSerializer();
        $this->kafkaConsumer = $this->createKafkaConsumer();


    }

    public function consume(array $topics, BaseRecord $record): void
    {
        try {
            $this->kafkaConsumer->subscribe($topics);
        } catch (Throwable $e) {
            print $e->getMessage() . "\n" . $e->getCode();
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
                print "NO error";
                return $this->handleNoError($message, $record);
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return $this->handlePartitionEof();
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                $this->handleTimeOut();
                break;
            default:
                print "error";
                $this->handleError($message, $record);
                break;
        }
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

    private function handleNoError(Message $message, BaseRecord $record)
    {
        try {

            $decoded = $this->serializer->deserialize($message->payload, $record);
            return ($this->successCallback)($decoded);
        } catch (AvroDecodingException $e) {
            $prev = $e->getPrevious();

            // parse the reader/writer types
            $matches = [];
            preg_match(
              '/Writer\'s schema .*?"name":"(\w+)".*?Reader\'s schema .*?"name":"(\w+)"/',
              $prev->getMessage(),
              $matches
            );
            [$_, $writerType, $readerType] = $matches;

            echo ">>> Skipping message. writerType: $writerType, readerType: $readerType" . PHP_EOL;
        } catch (Throwable $t) {
            // ....
        }

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
            ($this->errorCallback)();
        }
        throw new KafkaException(sprintf('%s for record %s', $message->errstr(), $record->name()), $message->err);
    }

    private function createKafkaConsumer(): KafkaConsumer
    {
        return new KafkaConsumer($this->config);
    }

    //    private function getPartitionsInfo()
    //    {
    //        $partitionsInfo = [];
    //
    //        foreach ($this->kafkaConsumer->getMetadata(true, null, 10000)->getTopics() as $topic) {
    //            $partitionsInfo[$topic->getTopic()] = count($topic->getPartitions());
    //        }
    //
    //        return $partitionsInfo;
    //    }
    //
    //    private function determineMaxPartitions(
    //      array $topics = null
    //    ) {
    //        $metaData = $this->kafkaConsumer->getMetadata(false, null, 12000);
    //        $allTopics = $metaData->getTopics();
    //        $max = 1;
    //        foreach ($allTopics as $topic) {
    //            /** @var \RdKafka\Metadata\Topic $topic */
    //            if (!in_array($topic->getTopic(), $topics)) {
    //                continue;
    //            }
    //            $numPartitions = count($topic->getPartitions());
    //            if ($numPartitions > $max) {
    //                $max = $numPartitions;
    //            }
    //        }
    //        return $max;
    //
    //    }
}