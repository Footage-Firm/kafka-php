<?php

namespace App\Producer;

use App\Common\TopicFormatter;
use App\Common\Utils;
use App\Events\BaseRecord;
use App\Producer\Exceptions\ProducerException;
use App\Serializers\KafkaSerializerInterface;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;


class Producer
{

    private $kafkaProducer;

    private $serializer;

    private $logger;

    public function __construct(
      KafkaProducer $kafkaProducer,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger
    ) {
        $this->serializer = $serializer;
        $this->kafkaProducer = $kafkaProducer;
        $this->logger = $logger;
    }

    public function produce(array $records, string $topic = '')
    {

        $this->validateProduceRequest($records, $topic);

        if (count($records) > 0) {
            $topic = $topic ?? TopicFormatter::topicFromRecord($records[0]);
        }

        $topicProducer = $this->kafkaProducer->newTopic($topic);

        foreach ($records as $record) {
            $encodedRecord = $this->encodeRecord($record);

            /*
             * RD_KAFKA_PARTITION_UA means kafka will automatically decide to which partition the record will be produced.
             * The second argument (msgflags) must always be 0 due to the underlying php-rdkafka implementation
             */
            $topicProducer->produce(RD_KAFKA_PARTITION_UA, 0, $encodedRecord);
            $this->kafkaProducer->poll(0);
        }

        while ($this->kafkaProducer->getOutQLen() > 0) {
            $this->kafkaProducer->poll(50);
        }
    }

    private function encodeRecord(BaseRecord $record): string
    {
        return $this->serializer->serialize($record);
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
}