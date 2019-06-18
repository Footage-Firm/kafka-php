<?php

namespace App\Consumer;

use App\Config;
use App\Serializers\KafkaSerializerInterface;
use Psr\Log\LoggerInterface;
use RdKafka\TopicConf;

class ConsumerConfig extends Config
{

    protected const DEFAULT_OFFSET = RD_KAFKA_OFFSET_BEGINNING;

    // todo - what is a sane default timeout?
    protected const DEFAULT_TIMEOUT = 1000;

    protected const DEFAULT_OFfSET_RESET = 'smallest';

    private $partition;

    private $offset;

    private $timeout;

    private $groupId;

    private $offsetReset;

    public function __construct(
      string $brokers,
      string $groupId,
      KafkaSerializerInterface $serializer,
      LoggerInterface $logger = null
    ) {
        parent::__construct($brokers, $serializer, $logger);
        $this->groupId = $groupId;
        $defaultTopicConfig = $this->createDefaultTopicConfig();
        $this->setDefaultTopicConf($defaultTopicConfig);
        $this->setGroupId($groupId);

    }

    public
    function setPartition(
      int $partition
    ) {
        $this->partition = $partition;
        return $this;
    }

    public
    function getOffset(): int
    {
        return $this->offset ?? static::DEFAULT_OFFSET;
    }

    public
    function setOffset(
      int $offset
    ) {
        $this->offset = $offset;
        return $this;
    }

    public
    function getTimeout(): int
    {
        return $this->timeout ?? static::DEFAULT_TIMEOUT;
    }

    public
    function setTimeout(
      int $timeout
    ) {
        $this->timeout = $timeout;
        return $this;
    }

    public
    function setOffsetReset(
      string $reset
    ) {
        $this->offsetReset = $reset;
    }

    public
    function getOffsetReset(): string
    {
        return $this->offsetReset ?? static::DEFAULT_OFfSET_RESET;
    }

    public
    function getGroupId(): string
    {
        return $this->groupId;
    }

    public
    function setGroupId(
      string $groupId
    ) {
        $this->groupId = $groupId;
        $this->set('group.id', $groupId);
        return $this;
    }

    private
    function createDefaultTopicConfig(): TopicConf
    {
        $topicConfig = new TopicConf();
        $topicConfig->set('auto.offset.reset', $this->getOffsetReset());
        return $topicConfig;
    }

}