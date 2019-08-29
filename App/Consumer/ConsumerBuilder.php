<?php

namespace App\Consumer;

use App\Common\ConfigOptions;
use App\Common\ConsumerConfigOptions;
use App\Common\KafkaBuilder;
use App\Consumer\Exceptions\ConsumerConfigurationException;
use App\Producer\Producer;
use App\Producer\ProducerBuilder;
use App\Serializers\KafkaSerializerInterface;
use FlixTech\SchemaRegistryApi\Registry;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;
use RdKafka\TopicConf;

class ConsumerBuilder extends KafkaBuilder
{

    public const DEFAULT_RETRIES = 3;

    public const MAX_RETRIES = 3;

    protected const DEFAULT_OFFSET_RESET = 'earliest';

    public const DEFAULT_TIMEOUT_MS = 1000;

    private $groupId;

    private $offsetReset;

    private $timeout;

    private $numRetries = self::DEFAULT_RETRIES;

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
        $this->config->set(ConsumerConfigOptions::GROUP_ID, $this->groupId);
        $this->topicConfig = $topicConf ?? $this->createDefaultTopicConfig();
        $this->disableAutoCommit();
    }

    public function build(): Consumer
    {
        $configDump = $this->config->dump();

        $this->config->setDefaultTopicConf($this->topicConfig);
        $kafkaConsumer = new KafkaConsumer($this->config);
        $failureProducer = $this->createFailureProducer($configDump);
        $recordProcessor = $this->createRecordProcessor($failureProducer);

        $consumer = new Consumer($kafkaConsumer, $this->logger, $recordProcessor);
        if ($this->timeout !== null) {
            $consumer->setTimeout($this->timeout);
        }

        return $consumer;
    }

    public function getOffsetReset(): string
    {
        return $this->offsetReset ?? static::DEFAULT_OFFSET_RESET;
    }

    public function setNumRetries(int $numRetries): self
    {
        if ($numRetries > self::MAX_RETRIES || $numRetries < 0) {
            throw new ConsumerConfigurationException(
              sprintf('Invalid retry number. Retries must be between 0 and %s', self::MAX_RETRIES)
            );
        }
        $this->numRetries = $numRetries;
        return $this;
    }

    public function setTimeout(int $timeout): self
    {
        $this->timeout = $timeout;
        return $this;
    }

    private function createDefaultTopicConfig(): TopicConf
    {
        $topicConfig = new TopicConf();
        $topicConfig->set(ConsumerConfigOptions::AUTO_OFFSET_RESET, $this->getOffsetReset());
        return $topicConfig;
    }

    protected function defaultTopicConfig(): TopicConf
    {
        return new TopicConf();
    }

    protected function disableAutoCommit(): self
    {
        $this->config->set(ConsumerConfigOptions::AUTO_COMMIT, 'false');
        $this->config->set(ConsumerConfigOptions::AUTO_COMMIT_INTERVAL, '0');
        $this->config->set(ConsumerConfigOptions::ENABLE_AUTO_OFFSET_STORE, 'false');
        return $this;
    }

    private function createRecordProcessor(Producer $failureProducer): RecordProcessor
    {
        $recordProcessor = new RecordProcessor($this->registry, $this->serializer, $this->groupId, $failureProducer);

        return $recordProcessor
          ->setNumRetries($this->numRetries)
          ->setShouldSendToFailureTopic($this->shouldSendToFailureTopic);
    }

    private function createFailureProducer(array $configDump)
    {

        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);

        if ($this->isUsingSsl($configDump)) {
            $builder->setSslData(
              $configDump[ConfigOptions::CA_PATH],
              $configDump[ConfigOptions::CERT_PATH],
              $configDump[ConfigOptions::KEY_PATH],
              );
        }

        return $builder->build();
    }

    private function isUsingSsl(array $configDump): bool
    {
        $necessaryKeys = [
          ConfigOptions::SECURITY_PROTOCOL,
          ConfigOptions::CERT_PATH,
          ConfigOptions::KEY_PATH,
          ConfigOptions::CA_PATH,
        ];
        return !array_diff_key(array_flip($necessaryKeys), $configDump)
          && $config[ConfigOptions::SECURITY_PROTOCOL] = ConfigOptions::SSL;
    }
}