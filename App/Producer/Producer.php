<?php

namespace App\Producer;

use App\Events\BaseRecord;
use RdKafka\Metadata;
use RdKafka\Producer as KafkaProducer;
use RdKafka\Topic;


class Producer
{

    private $config;

    private $serializer;

    private $kafkaProducer;

    public function __construct(ProducerConfig $config)
    {
        $this->config = $config;
        $this->serializer = $config->getSerializer();
        $this->kafkaProducer = $this->createKafkaProducer();
    }

    public function produce(string $topic, BaseRecord $record): void
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

    public function produceMany(string $topic, array $records)
    {
        $topicProducer = $this->kafkaProducer->newTopic($topic);
        foreach ($records as $record) {
            $encodedRecord = $this->encodeRecord($record);
            $topicProducer->produce(
              $this->config->getPartition(),
              $this->config->getMessageFlag(),
              $encodedRecord,
              $record->getKey()
            );
            $this->kafkaProducer->poll(0);
        }

        while ($this->kafkaProducer->getOutQLen() > 0) {
            $this->kafkaProducer->poll(50);
        }
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