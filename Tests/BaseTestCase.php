<?php


namespace Tests;

use Dotenv\Dotenv;
use PHPUnit\Framework\TestCase;

abstract class BaseTestCase extends TestCase
{
    use WithFaker;

    protected $env;
    protected $brokerHosts;
    protected $schemaRegistryUrl;
    protected $redisHost;

    protected function setUp(): void
    {
        $ROOT = __DIR__ . '/../';
        $dotenv = Dotenv::create($ROOT);
        $dotenv->load();
        $dotenv->required(['BROKER_HOSTS','SCHEMA_REGISTRY_URL', 'REDIS_HOST']);
        $this->brokerHosts = preg_split('/,/', getenv('BROKER_HOSTS'));
        $this->schemaRegistryUrl = getenv('SCHEMA_REGISTRY_URL');
        $this->redisHost = getenv('REDIS_HOST');
        $this->env = getenv('ENV');
    }

}