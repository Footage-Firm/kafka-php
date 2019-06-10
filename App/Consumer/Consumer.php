<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\SerializerFactory;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use RdKafka\Exception as KafkaException;
use RdKafka\KafkaConsumer;
use RdKafka\KafkaConsumerTopic;
use RdKafka\Message;
use RdKafka\Metadata;
use Throwable;

class Consumer
{

    private $serializer;

    private $kafkaConsumer;

    private $config;

    private $errorCallback;

    private $successCallback;

    private $topics;

    public function __construct(ConsumerConfig $config, array $topics, RecordSerializer $serializer = null)
    {
        $this->config = $config;
        $this->topics = $topics;
        $this->serializer = $serializer ?? (new SerializerFactory($config))->instance();
        $this->kafkaConsumer = new KafkaConsumer($this->config);
        $this->kafkaConsumer->subscribe($topics);
    }

    public function consumeSingle(BaseRecord $record)
    {
        $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        return $this->handleMessage($message, $record);
    }

    public function consumeUntilEnd(BaseRecord $record): array
    {
        $results = [];

        $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        print "initial msg $message->err";
        while ($message->err !== RD_KAFKA_RESP_ERR__PARTITION_EOF) {
            $results[] = $this->handleMessage($message, $record);
            $message = $this->kafkaConsumer->consume($this->config->getTimeout());
        }

        return $results;
    }

    public function consume(BaseRecord $record): void
    {
        while (true) {
            $message = $this->kafkaConsumer->consume($this->config->getTimeout());
            $this->handleMessage($message, $record);
        }
    }

    private function handleMessage(Message $message, BaseRecord $record)
    {
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $this->handleNoError($message, $record);
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
            $decoded = $this->serializer->decodeMessage($message->payload);
            $record->decode($decoded);
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
        }
        try {
            return ($this->successCallback)($record);
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