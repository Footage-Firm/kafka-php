<?php

namespace Test\Producer\Integration;

use App\Producer\ProducerBuilder;
use EventsPhp\Storyblocks\Common\DebugRecord;
use Exception;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Tests\Fakes\FakeFactory;
use Tests\Fakes\FakeRecord;

class TestProducer extends TestCase
{

    private $schemaRegistryUrl = "http://0.0.0.0:8081";

    private $brokers = ["0.0.0.0:29092"];

    public function setUp(): void
    {
        $this->brokers = getenv('KAFKA_URL') ? [getenv('KAFKA_URL')] : $this->brokers;
        $this->schemaRegistryUrl = getenv('SCHEMA_REGISTRY_URL') ?: $this->schemaRegistryUrl;
    }

    public function testProduceRecord()
    {
        $this->expectNotToPerformAssertions();
        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);
        $producer = $builder->build();
        $record = new DebugRecord();
        $record->setPayload("kafka-php unit test");
        $producer->produce($record);
    }

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
}