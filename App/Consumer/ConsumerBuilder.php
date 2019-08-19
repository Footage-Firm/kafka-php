<?php

namespace App\Consumer;

use App\Common\KafkaBuilder;
use App\Serializers\KafkaSerializerInterface;
use FlixTech\SchemaRegistryApi\Registry;
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

    private $timeout;

    private $groupId;

    private $offsetReset;


    public function __construct(
      array $brokers,
      string $groupId,
      string $schemaRegistryUrl,
      LoggerInterface $logger = null,
      Conf $config = null,
      TopicConf $topicConf = null,
      Registry $registry = null,
      KafkaSerializerInterface $serializer = null

    ) {
        parent::__construct($brokers, $schemaRegistryUrl, $logger, $config, $topicConf, $registry, $serializer);
        $this->groupId = $groupId;
        $this->topicConfig = $topicConf ?? $this->createDefaultTopicConfig();
        $this->setGroupId($groupId);
        $this->disableAutoCommit();
    }

    public function build(): Consumer
    {
        $this->config->setDefaultTopicConf($this->topicConfig);
        $kafkaConsumer = new KafkaConsumer($this->config);
        return new Consumer($kafkaConsumer, $this->serializer, $this->logger, $this->registry);
    }

    public function getOffsetReset(): string
    {
        return $this->offsetReset ?? static::DEFAULT_OFfSET_RESET;
    }

    public function setGroupId(string $groupId)
    {
        $this->groupId = $groupId;
        $this->config->set('group.id', $groupId);
        return $this;
    }

    private function createDefaultTopicConfig(): TopicConf
    {
        $topicConfig = new TopicConf();
        $topicConfig->set('auto.offset.reset', $this->getOffsetReset());
        return $topicConfig;
    }

    protected function defaultTopicConfig(): TopicConf
    {
        return new TopicConf();
    }

    protected function disableAutoCommit()
    {
        $this->config->set('auto.commit.enable', 'false');
        $this->config->set('auto.commit.interval.ms', '0');
        $this->config->set('enable.auto.offset.store', 'false');
    }
}