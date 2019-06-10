<?php

namespace App\Producer;

use App\Events\BaseRecord;
use App\SerializerFactory;
use AvroSchema;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use RdKafka\Producer as KafkaProducer;
use RdKafka\Topic;


class Producer
{

    private $config;

    private $serializer;

    private $kafkaProducer;

    public function __construct(ProducerConfig $config, RecordSerializer $serializer = null)
    {
        $this->config = $config;
        $this->serializer = $serializer ?? (new SerializerFactory($config))->instance();
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

    public function getMetaData(bool $allTopics, ?Topic $onlyTopic, int $timeoutMs)
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
        $schema = AvroSchema::parse($record->schema());
        $data = $record->data();
        $name = str_replace('_', '-', $this->convertToSnakeCase($record->name()));

        return $this->serializer->encodeRecord($name . '-value', $schema, $data);
    }

    private function convertToSnakeCase($value, $delimiter = '_')
    {
        if (!ctype_lower($value)) {
            $value = preg_replace('/\s+/u', '', ucwords($value));

            $value = preg_replace('/(.)(?=[A-Z])/u', '$1' . $delimiter, $value);
            return mb_strtolower($value, 'UTF-8');
        }

        return $value;
    }

}