<?php

namespace Test\Producer\Integration;

use KafkaPhp\Producer\ProducerBuilder;
use Exception;
use KafkaPhp\Serializers\Errors\SchemaRegistryError;
use RuntimeException;
use Tests\BaseTestCase;
use Tests\Util\Fakes\FakeFactory;
use Tests\Util\Fakes\FakeRecord;

class ProducerTest extends BaseTestCase
{

    public function testExceptionThrown_WhenFailsToEncode()
    {
        $this->expectException(Exception::class);
        $builder = new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl);
        $producer = $builder->build();
        $record = new FakeRecord();
        $record->setId('test');
        $producer->produce($record);
    }

    public function testExceptionThrown_WhenSchemaRegUrlIsWrong()
    {
        $this->expectException(SchemaRegistryError::class);
        $builder = new ProducerBuilder($this->brokerHosts, 'http://not.gonna.work');

        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }

    public function testConnectTimeout()
    {
        $builder = new ProducerBuilder(['fake.host:1337'], $this->schemaRegistryUrl);
        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }
}