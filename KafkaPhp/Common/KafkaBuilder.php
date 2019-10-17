<?php

namespace KafkaPhp\Common;

use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use KafkaPhp\Serializers\AvroSerializer;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\Producer;
use RdKafka\TopicConf;

abstract class KafkaBuilder
{

    /** @var Conf */
    protected $config;

    /** @var TopicConf */
    protected $topicConfig;

    /** @var KafkaSerializerInterface */
    protected $serializer;

    /** @var LoggerInterface */
    protected $logger;

    protected $shouldSendToFailureTopic = true;

    /** @var Registry */
    protected $registry;

    /** @var string[] */
    protected $brokers;

    protected $schemaRegistryUrl;

    abstract public function build();

    public function __construct(
      array $brokers,
      string $schemaRegistryUrl,
      LoggerInterface $logger = null,
      Conf $config = null,
      TopicConf $topicConfig = null,
      Registry $registry = null,
      KafkaSerializerInterface $serializer = null
    ) {
        $this->brokers = $brokers;
        $this->schemaRegistryUrl = $schemaRegistryUrl;
        $this->registry = $registry ?? $this->createRegistry($schemaRegistryUrl);
        $this->serializer = $serializer ?? new AvroSerializer($this->registry, true, true);
        $this->logger = $logger ?? new Logger('kafka');
        $this->config = $config ?? new Conf();
        $this->topicConfig = $topicConfig ?? new TopicConf();
        $this->config->set(ConfigOptions::BROKER_LIST, implode(',', $brokers));
        $this->config->setErrorCb([$this, 'defaultErrorCallback']);
    }

    public function setSslData(string $caPath, string $certPath, string $keyPath): self
    {
        $this->config->set(ConfigOptions::SECURITY_PROTOCOL, 'ssl');
        $this->config->set(ConfigOptions::CA_PATH, $caPath);
        $this->config->set(ConfigOptions::CERT_PATH, $certPath);
        $this->config->set(ConfigOptions::KEY_PATH, $keyPath);

        return $this;
    }

    //The callback has a signature of function (Rdkafka\Producer $kafka, int $err, string $reason);
    public function setKafkaErrorCallback(callable $callback): self
    {
        $this->config->setErrorCb($callback);
        return $this;
    }

    public function setStatsCallback(callable $callback): self
    {
        $this->config->setStatsCb($callback);
        return $this;
    }

    public function enableDebug(): self
    {
        $this->config->set('debug', 'all');
        return $this;
    }

    public function shouldSendToFailureTopic(bool $shouldSendToFailureTopic): self
    {
        $this->shouldSendToFailureTopic = $shouldSendToFailureTopic;
        return $this;
    }

    private function createRegistry(string $schemaRegistryUrl): Registry
    {
        $config = ['base_uri' => $schemaRegistryUrl];

        $user = parse_url($schemaRegistryUrl, PHP_URL_USER);
        $pass = parse_url($schemaRegistryUrl, PHP_URL_PASS);

        if ($user || $pass) {
            $config['auth'] = [$user, $pass];
        }
        $client = new Client($config);
        return new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
    }

    /**
     * @param Producer|Consumer $kafka
     * @param int $err
     * @param string $reason
     */
    public function defaultErrorCallback($kafka, int $err, string $reason)
    {
        throw new \RuntimeException($reason, $err);
    }
}