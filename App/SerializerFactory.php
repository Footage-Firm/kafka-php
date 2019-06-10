<?php

namespace App;

use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;

// todo -- not really a factory
class SerializerFactory
{

    private $config;

    private $registry;

    public function __construct(Config $config, Registry $registry = null)
    {
        $this->config = $config;
        $this->registry = $registry ?? $this->createRegistry();
    }

    public function instance(): RecordSerializer
    {
        return new RecordSerializer(
          $this->registry,
          [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => $this->config->shouldRegisterMissingSchemas(),
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => $this->config->shouldRegisterMissingSubjects(),
          ]
        );
    }

    private function createRegistry(): Registry
    {
        return new CachedRegistry(
          new PromisingRegistry(
            new Client(['base_uri' => $this->config->getSchemaRegistryUri()])
          ),
          new AvroObjectCacheAdapter()
        );
    }
}