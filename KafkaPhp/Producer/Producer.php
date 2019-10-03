<?php

namespace KafkaPhp\Producer;

use Carbon\Carbon;
use EventsPhp\Util\EventFactory;
use KafkaPhp\Common\KafkaListener;
use KafkaPhp\Common\TopicFormatter;
use KafkaPhp\Producer\Errors\ProducerTimeoutError;
use KafkaPhp\Serializers\Errors\SchemaRegistryError;
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

    /** @var int */
    private $timeoutMs;

    public function __construct(
      KafkaProducer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger,
      int $timeoutMs
    ) {
        $this->serializer = $serializer;
        $this->kafkaClient = $kafkaClient;
        $this->logger = $logger;
        $this->timeoutMs = $timeoutMs;
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
        } catch (SchemaRegistryError $e) {
            // Propagate a schema registry error and do not retry.
            throw $e;
        } catch (Throwable $t) {
            if ($produceFailureRecords) {
                $this->produceFailureRecord($record, $topic, $t->getMessage());
            }
            $this->logger->error($t->getMessage());
            throw $t;
        }

        $start = Carbon::now();
        while ($this->kafkaClient->getOutQLen() > 0) {
            $this->kafkaClient->poll(100);
            if (Carbon::now()->diffInMilliseconds($start) >= $this->timeoutMs) {
                throw new ProducerTimeoutError("Producer timed out sending message");
            }
        }
    }

    private function produceFailureRecord(BaseRecord $record, string $topic, string $errorMsg): void
    {
        $failedRecord = EventFactory::failedRecord($topic, $record, $errorMsg);
        $this->produce($failedRecord, TopicFormatter::producerFailureTopic($topic), false);
    }
}