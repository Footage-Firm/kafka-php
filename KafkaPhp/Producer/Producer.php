<?php

namespace KafkaPhp\Producer;

use EventsPhp\Storyblocks\Common\Origin;
use EventsPhp\Util\EventFactory;
use KafkaPhp\Common\TopicFormatter;
use KafkaPhp\Serializers\Exceptions\SchemaRegistryException;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use EventsPhp\BaseRecord;
use Psr\Log\LoggerInterface;
use Ramsey\Uuid\Uuid;
use RdKafka\Producer as KafkaProducer;
use RdKafka\TopicConf;
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

    /** @var Origin */
    private $origin;

    public function __construct(
      KafkaProducer $kafkaClient,
      KafkaSerializerInterface $serializer,
      Origin $origin,
      LoggerInterface $logger,
      ?int $timeoutMs = ProducerBuilder::DEFAULT_TIMEOUT_MS
    ) {
        $this->serializer = $serializer;
        $this->kafkaClient = $kafkaClient;
        $this->logger = $logger;
        $this->origin = $origin;
        $this->timeoutMs = $timeoutMs;
    }

    /**
     * Produce a record. If no key is provided, a uuid will be generated and sent as the key.
     * @param BaseRecord $record
     * @param string|null $key
     * @param string|null $topic
     * @param bool $produceFailureRecords
     * @throws SchemaRegistryException
     * @throws Throwable
     */
    public function produce(BaseRecord $record, string $key = null, string $topic = null, bool $produceFailureRecords = true): void
    {
        $topic = $topic ?? TopicFormatter::topicFromRecord($record);

        // To ensure every message is produced with a key, generate a uuid if one is not provided.
        $key = $key ?? Uuid::uuid4()->toString();

        $topicConf = new TopicConf();
        $topicConf->set('message.timeout.ms', $this->timeoutMs);
        $topicProducer = $this->kafkaClient->newTopic($topic, $topicConf);

        try {
            $encodedRecord = $this->serializer->serialize($record);
            /*
             * RD_KAFKA_PARTITION_UA means kafka will automatically decide to which partition the record will be produced.
             * The second argument (msgflags) must always be 0 due to the underlying php-rdkafka implementation
             */
            $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord, $key);
        } catch (SchemaRegistryException $e) {
            // Propagate a schema registry error and do not retry.
            throw $e;
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
        $failedRecord = EventFactory::failedRecord($record, $topic, $this->origin, $errorMsg);
        $this->produce($failedRecord, null, TopicFormatter::producerFailureTopic($topic), false);
    }
}