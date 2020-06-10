<?php

namespace KafkaPhp\Consumer;

use EventsPhp\Storyblocks\Common\Origin;
use KafkaPhp\Common\ConfigOptions;
use KafkaPhp\Common\ConsumerConfigOptions;
use KafkaPhp\Common\KafkaBuilder;
use KafkaPhp\Consumer\Exceptions\ConsumerConfigurationException;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Producer\ProducerBuilder;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\KafkaConsumer;

class ConsumerBuilder extends KafkaBuilder
{

    public const DEFAULT_RETRIES = 3;
    public const MAX_RETRIES = 3;
    public const DEFAULT_OFFSET_RESET = 'earliest';
    public const DEFAULT_TIMEOUT_MS = 3000;
    public const DEFAULT_POLL_INTERVAL_MS = 100;
    public const DEFAULT_AUTO_COMMIT_INTERVAL_MS = 5000;
    public const DEFAULT_HEARTBEAT_INTERVAL_MS = 3000;
    public const DEFAULT_FETCH_WAIT_MAX_MS = 2000;
    public const DEFAULT_SOCKET_TIMEOUT_MS = 120000;

    private $numRetries = self::DEFAULT_RETRIES;
    private $connectTimeoutMs = self::DEFAULT_TIMEOUT_MS;
    private $pollIntervalMs = self::DEFAULT_POLL_INTERVAL_MS;
    private $autoCommitInterval = self::DEFAULT_AUTO_COMMIT_INTERVAL_MS;
    private $heartbeatIntervalMs = self::DEFAULT_HEARTBEAT_INTERVAL_MS;

    /** @var null | int */
    private $idleTimeoutMs = null;

    private $groupId;

    public function __construct(
        array $brokers,
        string $groupId,
        string $schemaRegistryUrl,
        Origin $origin,
        LoggerInterface $logger = null,
        Conf $config = null
    )
    {
        parent::__construct($brokers, $schemaRegistryUrl, $origin, $logger, $config);
        $this->groupId = $groupId;
        $this->config->set(ConsumerConfigOptions::GROUP_ID, $groupId);
        $this->config->set(ConsumerConfigOptions::AUTO_OFFSET_RESET, self::DEFAULT_OFFSET_RESET);
        $this->config->set(ConfigOptions::RETRIES, 2);
        $this->config->set(ConsumerConfigOptions::FETCH_WAIT_MAX_MS, self::DEFAULT_FETCH_WAIT_MAX_MS);
        $this->config->set(ConfigOptions::SOCKET_TIMEOUT_MS, self::DEFAULT_SOCKET_TIMEOUT_MS);
        $this->config->set(ConsumerConfigOptions::TOPIC_METADATA_REFRESH_SPARSE, "true");
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

    public function setOffsetReset(string $offsetReset): self
    {
        $this->config->set(ConsumerConfigOptions::AUTO_OFFSET_RESET, $offsetReset);
        return $this;
    }

    public function setHeartbeatIntervalMs(int $heartbeatIntervalMs=null): self
    {
        if (!$heartbeatIntervalMs) {
            $heartbeatIntervalMs = self::DEFAULT_HEARTBEAT_INTERVAL_MS;
        }
        $this->config->set(ConfigOptions::HEARTBEAT_INTERVAL_MS, $heartbeatIntervalMs);
        $this->heartbeatIntervalMs = $heartbeatIntervalMs;
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

    public function enableAutoCommit(int $autoCommitInterval = null): self
    {
        if (!$autoCommitInterval) {
            $autoCommitInterval = self::DEFAULT_AUTO_COMMIT_INTERVAL_MS;
        }
        $this->autoCommitInterval = $autoCommitInterval;
        $this->config->set(ConsumerConfigOptions::AUTO_COMMIT_INTERVAL, $autoCommitInterval);
        return $this;
    }

    public function disableAutoCommit(): self
    {
        $this->autoCommitInterval = 0;
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
        } elseif ($this->isUsingSasl($configDump)) {
            $builder->setSaslData(
                $configDump[ConfigOptions::SASL_USERNAME],
                $configDump[ConfigOptions::SASL_PASSWORD],
                $configDump[ConfigOptions::SASL_MECHANISM]
            );
        }

        $builder->setVerifyRegistrySsl($this->verifyRegistrySsl);

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
            && $configDump[ConfigOptions::SECURITY_PROTOCOL] === 'ssl';
    }

    private function isUsingSasl(array $configDump): bool
    {
        return $configDump[ConfigOptions::SECURITY_PROTOCOL] === 'sasl_ssl';
    }

    public function setIdleTimeoutMs(int $idleTimeoutMs): ConsumerBuilder
    {
        $this->idleTimeoutMs = $idleTimeoutMs;
        return $this;
    }
}