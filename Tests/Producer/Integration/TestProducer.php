<?php

namespace Test\Producer\Integration;

use App\Producer\ProducerBuilder;
use AvroSchemaParseException;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Fakes\FakeFactory;
use Tests\Fakes\FakeRecord;
use Tests\WithFaker;

class TestProducer extends TestCase
{

    use WithFaker;

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();
    }

    private $schemaRegistryUrl = "http://0.0.0.0:8081";

    private $brokers = ["0.0.0.0:29092"];

    public function testExceptionThrown_WhenFailsToEncode()
    {
        $this->expectException(AvroSchemaParseException::class);
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
}