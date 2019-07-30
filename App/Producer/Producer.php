<?php

namespace App\Producer;

use App\Common\KafkaListener;
use App\Common\TopicFormatter;
use App\Common\Utils;
use App\Events\BaseRecord;
use App\Producer\Exceptions\ProducerException;
use App\Records\Failure\Failure;
use App\Serializers\KafkaSerializerInterface;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use Throwable;


class Producer
{

    /** @var KafkaProducer */
    private $kafkaClient;

    /** @var KafkaSerializerInterface */
    private $serializer;

    public function __construct(
      KafkaProducer $kafkaClient,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger
    ) {
        $this->serializer = $serializer;
        $this->kafkaClient = $kafkaClient;
    }

    public function produce(array $records, string $topic = '', bool $produceFailureRecords = true)
    {
        $this->validateProduceRequest($records, $topic);

        if (count($records) > 0) {
            $topic = $topic ?? TopicFormatter::topicFromRecord($records[0]);
        }

        $topicProducer = $this->kafkaClient->newTopic($topic);

        foreach ($records as $record) {
            $encodedRecord = $this->encodeRecord($record, $topic, $produceFailureRecords);

            /*
             * RD_KAFKA_PARTITION_UA means kafka will automatically decide to which partition the record will be produced.
             * The second argument (msgflags) must always be 0 due to the underlying php-rdkafka implementation
             */
            $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord);
            $this->kafkaClient->poll(2000);
        }

        while ($this->kafkaClient->getOutQLen() > 0) {
            $this->kafkaClient->poll(50);
        }
    }


    private function encodeRecord(BaseRecord $record, string $topic, bool $produceErrorRecords): string
    {
        try {
            return $this->serializer->serialize($record);
        } catch (Throwable $t) {
            $errorMsg = 'Encoding error: ' . $t->getMessage();
            if ($produceErrorRecords) {
                $this->produceFailureRecord($record, $topic, $errorMsg);
            } else {
                $this->logger->warning($errorMsg);
            }
        }
    }

    /**
     * @param  BaseRecord[]  $records
     * @param  string  $topic
     *
     * @throws \App\Producer\Exceptions\ProducerException
     */
    private function validateProduceRequest(array $records, string $topic): void
    {
        if (!$topic && count($records) === 0) {
            throw new ProducerException('Unable to use default topic when no records are being produced');
        }

        array_reduce($records, function ($prev, $cur)
        {
            if ($prev && Utils::className($cur) !== Utils::className($prev)) {
                throw new ProducerException('Cannot produce different types of records at the same time.');
            }
            return $cur;
        });
    }

    private function produceFailureRecord(BaseRecord $record, string $topic, string $errorMsg): void
    {
        $failure = new Failure();
        $failure->setPayload(json_encode($record));
        $failure->setFailedTopic($topic);
        $failure->setDetails($errorMsg);

        $this->produce([$failure], TopicFormatter::producerFailureTopicFromRecord($record), false);
    }
}