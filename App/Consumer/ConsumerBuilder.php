<?php

namespace App\Consumer;

use App\Common\KafkaBuilder;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicConf;

class ConsumerBuilder extends KafkaBuilder
{

    protected const DEFAULT_OFFSET = RD_KAFKA_OFFSET_BEGINNING;

    // todo - what is a sane default timeout?
    protected const DEFAULT_TIMEOUT = 1000;

    protected const DEFAULT_OFfSET_RESET = 'earliest';

    private $partition;

    private $timeout;

    private $groupId;

    private $offsetReset;

    private $topicConfig;

    public function __construct(
      array $brokers,
      string $groupId,
      string $schemaRegistryUrl,
      LoggerInterface $logger,
      Conf $config = null,
      TopicConf $topicConf = null
    ) {
        parent::__construct($brokers, $schemaRegistryUrl, $logger, $config);
        $this->groupId = $groupId;
        $defaultTopicConfig = $this->createDefaultTopicConfig();
        $this->setGroupId($groupId);

    }

    public function build(): Consumer
    {
        $kafkaConsumer = new KafkaConsumer($this->config);
        return new Consumer($kafkaConsumer, $this->serializer, $this->logger);
    }

    public function getOffsetReset(): string
    {
        return $this->offsetReset ?? static::DEFAULT_OFfSET_RESET;
    }

    public function setGroupId(string $groupId)
    {
        $this->groupId = $groupId;
        $this->set('group.id', $groupId);
        return $this;
    }


    private function createDefaultTopicConfig(): TopicConf
    {
        $topicConfig = new TopicConf();
        $topicConfig->set('auto.offset.reset', $this->getOffsetReset());
        return $topicConfig;
    }

}