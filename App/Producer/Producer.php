<?php

namespace App\Producer;

use App\Events\BaseRecord;
use App\Serializers\KafkaSerializerInterface;
use RdKafka\Metadata;
use RdKafka\Producer as KafkaProducer;
use RdKafka\Topic;


class Producer
{

    private $config;

    private $serializer;

    private $kafkaProducer;

    public function __construct(ProducerConfig $config, KafkaSerializerInterface $serializer)
    {
        $this->config = $config;
        $this->serializer = $serializer;
        $this->kafkaProducer = $this->createKafkaProducer();
    }

    public function fire(string $topic, BaseRecord $record): void
    {
        $topicProducer = $this->kafkaProducer->newTopic($topic);
        $encodedRecord = $this->encodeRecord($record);
        $topicProducer->produce(
          $this->config->getPartition(),
          $this->config->getMessageFlag(),
          $encodedRecord,
          $record->getKey()
        );
        $this->kafkaProducer->poll(0);

    }

    public function getMetaData(bool $allTopics, ?Topic $onlyTopic, int $timeoutMs): Metadata
    {
        return $this->kafkaProducer->getMetadata($allTopics, $onlyTopic, $timeoutMs);
    }

    private function createKafkaProducer(): KafkaProducer
    {
        $producer = new KafkaProducer($this->config);

        $logLevel = $this->config->getLogLevel();
        if ($logLevel) {
            $producer->setLogLevel(LOG_DEBUG);
        }

        return $producer;
    }

    private function encodeRecord(BaseRecord $record): string
    {
        return $this->serializer->serialize($record);
    }
}