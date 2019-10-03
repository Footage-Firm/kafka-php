<?php

namespace Test\Producer\Integration;

use KafkaPhp\Producer\ProducerBuilder;
use Exception;
use RuntimeException;
use Tests\BaseTest;
use Tests\Util\Fakes\FakeFactory;
use Tests\Util\Fakes\FakeRecord;

class TestProducer extends BaseTest
{

    private $schemaRegistryUrl = "http://0.0.0.0:8081";

    private $brokers = ["0.0.0.0:29092"];

    public function testExceptionThrown_WhenFailsToEncode()
    {
        $this->expectException(Exception::class);
        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);
        $producer = $builder->build();
        $record = new FakeRecord();
        $record->setId('test');
        $producer->produce($record);
    }

    public function testExceptionThrown_WhenSchemaRegUrlIsWrong()
    {
        $this->expectException(RuntimeException::class);
        $builder = new ProducerBuilder($this->brokers, 'http://not.gonna.work');

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