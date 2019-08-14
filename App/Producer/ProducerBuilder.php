<?php

namespace App\Producer;

use App\Common\KafkaBuilder;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\Producer as KafkaProducer;
use RdKafka\TopicConf;

class ProducerBuilder extends KafkaBuilder
{

    public const DEFAULT_RETRIES = 3;
    
    public function __construct(
      array $brokers,
      string $schemaRegistryUrl,
      LoggerInterface $logger = null,
      Conf $config = null
    ) {
        parent::__construct($brokers, $schemaRegistryUrl, $logger, $config);

        $this->config->set('retries', self::DEFAULT_RETRIES);
        $this->config->set('acks', 'all'); //todo -- comment/constant so this is clear

        // From docs, capture this someplace: When set to true, the producer will ensure that messages are successfully produced exactly once
        // and in the original produce order. The following configuration properties are adjusted automatically (if not
        // modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than
        // or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer
        // instantiation will fail if user-supplied configuration is incompatible.
        $this->config->set('enable.idempotence', true);

    }

    public function build(): Producer
    {
        $kafkaProducer = new KafkaProducer($this->config);

        return new Producer($kafkaProducer, $this->serializer, $this->logger);
    }

    // Signature of the callback function is function (RdKafka\RdKafka $kafka, RdKafka\Message $message);
    public function setDeliveryReportCallback(callable $callback): self
    {
        $this->config->setDrMsgCb($callback);
        return $this;
    }

    public function setNumRetries(int $numRetries): self
    {
        $this->config->set('retries', self::DEFAULT_RETRIES);
        return $this;
    }


    protected function defaultTopicConfig(): TopicConf
    {
        return new TopicConf();
    }
}