<?php

namespace App\Common;

use App\Serializers\AvroSerializer;
use App\Serializers\KafkaSerializerInterface;
use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use Monolog\Logger;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
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

    protected $shouldProduceFailureRecords = true;

    /** @var Registry */
    protected $registry;

    abstract protected function defaultTopicConfig(): TopicConf;

    public function __construct(
      array $brokers,
      string $schemaRegistryUrl,
      LoggerInterface $logger = null,
      Conf $config = null,
      TopicConf $topicConfig = null,
      Registry $registry = null,
      KafkaSerializerInterface $serializer = null
    ) {
        $this->registry = $registry ?? $this->createRegistry($schemaRegistryUrl);
        $this->serializer = $serializer ?? new AvroSerializer($this->registry, true, true);
        $this->logger = $logger ?? new Logger('kafka');
        $this->config = $config ?? new Conf();
        $this->topicConfig = $topicConfig ?? $this->defaultTopicConfig();
        $this->config->set('metadata.broker.list', implode(',', $brokers));
    }

    public function setSslData(string $caPath, string $certPath, string $keyPath): void
    {
        $this->config->set('security.protocol', 'ssl');
        $this->config->set('ssl.ca.location', $caPath);
        $this->config->set('ssl.certificate.location', $certPath);
        $this->config->set('ssl.key.location', $keyPath);
    }

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

    public function shouldProduceFailureRecords(bool $shouldProduceFailureRecords): self
    {
        $this->shouldProduceFailureRecords = $shouldProduceFailureRecords;
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
}