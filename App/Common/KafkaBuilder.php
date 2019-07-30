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

abstract class KafkaBuilder
{

    protected $config;

    protected $serializer;

    protected $logger;

    public function __construct(array $brokers, Conf $config, string $schemaRegistryUrl, LoggerInterface $logger)
    {
        $this->config = $config ?? new Conf();
        $this->serializer = $this->createSerializer($schemaRegistryUrl);
        $this->logger = $logger;

        $this->config->set('metadata.broker.list', implode(',', $brokers));
    }

    public function onKafkaError(callable $callback): KafkaBuilder
    {
        $this->config->setErrorCb($callback);
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