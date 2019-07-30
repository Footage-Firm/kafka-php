<?php

namespace App\Common;

use App\Serializers\AvroSerializer;
use App\Serializers\KafkaSerializerInterface;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\TopicConf;

abstract class KafkaBuilder
{

    protected $config;

    protected $topicConfig;

    protected $serializer;

    protected $logger;

    public function __construct(
      array $brokers,
      string $schemaRegistryUrl,
      LoggerInterface $logger,
      Conf $config = null,
      TopicConf $topicConfig = null
    ) {
        $this->serializer = $this->createSerializer($schemaRegistryUrl);
        $this->logger = $logger;
        $this->config = $config ?? new Conf();
        $this->topicConfig = $topicConfig ?? new TopicConf();
        $this->config->set('metadata.broker.list', implode(',', $brokers));
    }

    public function onKafkaError(callable $callback): self
    {
        $this->config->setErrorCb($callback);
        return $this;
    }

    public function setStatsCallback(callable $callback): self
    {
        $this->config->setStatsCb($callback);
        return $this;
    }

    private function createSerializer(string $schemaRegistryUrl): KafkaSerializerInterface
    {
        $client = new Client([
          'base_uri' => $schemaRegistryUrl,
            //todo -- parse the url for auth
            //          'auth' => [$schemaRegistryUser, $schemaRegistryPassword],
        ]);
        $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
        // todo -- disable new schema creation
        return new AvroSerializer($registry, true, true);
    }

}