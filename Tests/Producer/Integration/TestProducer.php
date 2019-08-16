<?php

namespace Test\Producer\Integration;

use App\Producer\Producer;
use App\Producer\ProducerBuilder;
use AvroSchemaParseException;
use RuntimeException;
use Tests\Fakes\FakeRecord;
use Tests\Fakes\FakeRecordFactory;
use Tests\TestCaseWithFaker;

class TestProducer extends TestCaseWithFaker
{

    private $schemaRegistryUrl = "http://0.0.0.0:8081";

    private $brokers = ["0.0.0.0:29092"];

    public function testExceptionThrown_WhenFailsToEncode()
    {
        $this->expectException(AvroSchemaParseException::class);
        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);
        /** @var Producer $producer */
        $producer = $builder->build();
        $record = new FakeRecord();
        $record->setId('test');
        $producer->produce($record);
    }

    public function testExceptionThrown_WhenSchemaRegUrlIsWrong()
    {
        $this->expectException(RuntimeException::class);
        $builder = new ProducerBuilder($this->brokers, 'http://not.gonna.work');

        /** @var Producer $producer */
        $producer = $builder->build();
        $producer->produce(FakeRecordFactory::fakeRecord());
    }
}