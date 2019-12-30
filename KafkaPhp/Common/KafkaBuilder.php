<?php

namespace KafkaPhp\Common;

use Doctrine\Common\Cache\RedisCache;
use EventsPhp\Storyblocks\Common\Origin;
use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\Cache\DoctrineCacheAdapter;
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
use Redis;

abstract class KafkaBuilder
{

    /** @var Conf */
    protected $config;

    /** @var KafkaSerializerInterface */
    protected $serializer;

    /** @var LoggerInterface */
    protected $logger;

    protected $shouldSendToFailureTopic = true;

    /** @var Redis */
    protected $redis;

    /** @var string[] */
    protected $brokers;

    /** @var Origin */
    protected $origin;

    protected $schemaRegistryUrl;

    abstract public function build();

    public function __construct(
      array $brokers,
      string $schemaRegistryUrl,
      Origin $origin,
      LoggerInterface $logger = null,
      Conf $config = null
    ) {
        $this->brokers = $brokers;
        $this->schemaRegistryUrl = $schemaRegistryUrl;
        $this->origin = $origin;
        $this->logger = $logger ?? new Logger('kafka');
        $this->config = $config ?? new Conf();
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

    public function setRedisSchemaCache(string $host, int $port = 6379)
    {
        $this->redis = new Redis();
        $this->redis->connect($host, $port);
        return $this;
    }

    protected function createSerializer(): KafkaSerializerInterface
    {
        $registry = $this->createRegistry();
        return $this->serializer ?? new AvroSerializer($registry, true, true);
    }

    private function createRegistry(): Registry
    {
        $config = ['base_uri' => $this->schemaRegistryUrl];

        $user = parse_url($this->schemaRegistryUrl, PHP_URL_USER);
        $pass = parse_url($this->schemaRegistryUrl, PHP_URL_PASS);

        if ($user || $pass) {
            $config['auth'] = [$user, $pass];
        }
        $client = new Client($config);

        $cacheAdapter = null;
        if ($this->redis) {
            $cache = new RedisCache();
            $cache->setRedis($this->redis);
            $cacheAdapter = new DoctrineCacheAdapter($cache);
        } else {
            $cacheAdapter = new AvroObjectCacheAdapter();
        }

        return new CachedRegistry(new PromisingRegistry($client), $cacheAdapter);
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