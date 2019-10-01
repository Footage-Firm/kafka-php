<?php

namespace KafkaPhp\Producer;

use EventsPhp\Util\EventFactory;
use KafkaPhp\Common\KafkaListener;
use KafkaPhp\Common\TopicFormatter;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use EventsPhp\BaseRecord;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use Throwable;


class Producer
{

    /** @var KafkaProducer */
    private $kafkaClient;

    /** @var KafkaSerializerInterface */
    private $serializer;

    /** @var \Psr\Log\LoggerInterface */
    private $logger;

    public function __construct(
      KafkaProducer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger
    ) {
        $this->serializer = $serializer;
        $this->kafkaClient = $kafkaClient;
        $this->logger = $logger;
    }

    public function produce(BaseRecord $record, string $topic = null, bool $produceFailureRecords = true): void
    {
        $topic = $topic ?? TopicFormatter::topicFromRecord($record);

        $topicProducer = $this->kafkaClient->newTopic($topic);

        try {
            $encodedRecord = $this->serializer->serialize($record);
            /*
             * RD_KAFKA_PARTITION_UA means kafka will automatically decide to which partition the record will be produced.
             * The second argument (msgflags) must always be 0 due to the underlying php-rdkafka implementation
             */
            $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord);
        } catch (Throwable $t) {
            if ($produceFailureRecords) {
                $this->produceFailureRecord($record, $topic, $t->getMessage());
            }
            $this->logger->error($t->getMessage());
            throw $t;
        }

        while ($this->kafkaClient->getOutQLen() > 0) {
            $this->kafkaClient->poll(100);
        }
    }

    private function produceFailureRecord(BaseRecord $record, string $topic, string $errorMsg): void
    {
        $failedRecord = EventFactory::failedRecord($topic, $record, $errorMsg);
        $this->produce($failedRecord, TopicFormatter::producerFailureTopic($topic), false);
    }
}