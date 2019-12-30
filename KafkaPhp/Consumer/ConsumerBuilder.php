<?php

namespace KafkaPhp\Consumer;

use EventsPhp\Storyblocks\Common\Origin;
use KafkaPhp\Common\ConfigOptions;
use KafkaPhp\Common\ConsumerConfigOptions;
use KafkaPhp\Common\KafkaBuilder;
use KafkaPhp\Consumer\Exceptions\ConsumerConfigurationException;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Producer\ProducerBuilder;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use FlixTech\SchemaRegistryApi\Registry;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class ConsumerBuilder extends KafkaBuilder
{

    public const DEFAULT_RETRIES = 3;

    public const MAX_RETRIES = 3;

    public const DEFAULT_OFFSET_RESET = 'earliest';

    public const DEFAULT_TIMEOUT_MS = 1000;

    public const DEFAULT_POLL_INTERVAL_MS = 10;

    private $offsetReset = self::DEFAULT_OFFSET_RESET;

    private $numRetries = self::DEFAULT_RETRIES;

    private $connectTimeoutMs = self::DEFAULT_TIMEOUT_MS;

    private $pollIntervalMs = self::DEFAULT_POLL_INTERVAL_MS;

    /** @var null | int */
    private $idleTimeoutMs = null;

    private $groupId;

    public function __construct(
      array $brokers,
      string $groupId,
      string $schemaRegistryUrl,
      Origin $origin,
      LoggerInterface $logger = null,
      Conf $config = null,
      Registry $registry = null,
      KafkaSerializerInterface $serializer = null
    ) {
        parent::__construct($brokers, $schemaRegistryUrl, $origin, $logger, $config, $registry, $serializer);
        $this->groupId = $groupId;
        $this->config->set(ConsumerConfigOptions::GROUP_ID, $this->groupId);
        $this->config->set(ConsumerConfigOptions::AUTO_OFFSET_RESET, $this->offsetReset);
        $this->config->set(ConfigOptions::RETRIES, 3);
        $this->disableAutoCommit();
    }

    public function build(): Consumer
    {
        $kafkaConsumer = new KafkaConsumer($this->config);
        $failureProducer = $this->createFailureProducer();
        $recordProcessor = $this->createRecordProcessor($failureProducer);

        return new Consumer(
          $kafkaConsumer,
          $this->createSerializer(),
          $this->logger,
          $recordProcessor,
          $this->idleTimeoutMs,
          $this->connectTimeoutMs,
          $this->pollIntervalMs
        );
    }

    public function setOffsetReset(string $offset): self
    {
        $this->offsetReset = $offset;
        return $this;
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

    public function setConnectTimeout(int $connectTimeoutMs): self
    {
        $this->connectTimeoutMs = $connectTimeoutMs;
        return $this;
    }

    public function setPollInterval(int $pollIntervalMs): self
    {
        $this->pollIntervalMs = $pollIntervalMs;
        return $this;
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
        $recordProcessor = new RecordProcessor($this->groupId, $failureProducer, $this->logger);

        return $recordProcessor
          ->setNumRetries($this->numRetries)
          ->setShouldSendToFailureTopic($this->shouldSendToFailureTopic);
    }

    private function createFailureProducer(): Producer
    {
        /**
         * This is the only way to access details of a Conf object
         *
         * @var array $configDump
         */
        $configDump = $this->config->dump();

        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl, $this->origin);

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

    public function setIdleTimeoutMs(int $idleTimeoutMs): ConsumerBuilder
    {
        $this->idleTimeoutMs = $idleTimeoutMs;
        return $this;
    }
}