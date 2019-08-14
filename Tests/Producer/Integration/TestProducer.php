<?php

namespace Test\Producer\Integration;

use App\Producer\Producer as ProducerAlias;
use App\Producer\ProducerBuilder;
use AvroSchemaParseException;
use Tests\Fakes\FakeRecord;
use Tests\TestCaseWithFaker;

class TestProducer extends TestCaseWithFaker
{

    private $producerBuilder;

    private $schemaRegistryUrl = "http://0.0.0.0:8081";

    private $brokers = ["0.0.0.0:29092"];

    public function setUp(): void
    {
        parent::setUp();
        $this->producerBuilder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);
    }

    public function testExceptionThrownWhenFailsToEncode()
    {
        $this->expectException(AvroSchemaParseException::class);
        /** @var ProducerAlias $producer */
        $producer = $this->producerBuilder->build();
        $record = new FakeRecord();
        $record->setId('test');
        $producer->produce($record);
    }
}