<?php

namespace Test\Producer\Integration;

use EventsPhp\Storyblocks\Common\Origin;
use KafkaPhp\Producer\ProducerBuilder;
use KafkaPhp\Serializers\Exceptions\SchemaRegistryException;
use Tests\BaseTestCase;
use Tests\Util\Fakes\FakeFactory;
use Tests\Util\Fakes\FakeRecord;

class ProducerTest extends BaseTestCase
{

    public function testExceptionThrown_WhenFailsToEncode()
    {
        $this->expectException(SchemaRegistryException::class);
        $builder = new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS());
        $producer = $builder->build();
        $record = new FakeRecord();
        $record->setId('test');
        $producer->produce($record);
    }

    public function testExceptionThrown_WhenSchemaRegUrlIsWrong()
    {
        $this->expectException(SchemaRegistryException::class);
        $builder = new ProducerBuilder($this->brokerHosts, 'http://not.gonna.work', Origin::VIDEOBLOCKS());

        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }

    public function testBrokerConnectionFailure()
    {
//        $this->expectException(\RuntimeException::class);
        $builder = new ProducerBuilder(['fake.host:1337'], $this->schemaRegistryUrl, Origin::VIDEOBLOCKS());
        $builder->setTimeoutMs(10);
        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }
}