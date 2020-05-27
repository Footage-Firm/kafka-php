<?php

namespace Test\Producer\Integration;

use EventsPhp\Storyblocks\Common\FailedRecord;
use EventsPhp\Storyblocks\Common\Origin;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroEncodingException;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Producer\ProducerBuilder;
use KafkaPhp\Serializers\Exceptions\SchemaRegistryException;
use Predis\Client;
use Tests\BaseTestCase;
use Tests\Utils\Factory;
use Tests\Utils\Fakes\FakeFactory;

class ProducerTest extends BaseTestCase
{

    public function testProduce()
    {
        $this->expectNotToPerformAssertions();
        $builder = new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::STORYBLOCKS());
        $producer = $builder->build();
        $producer->produce(Factory::debugRecord('[kafka-php test] ' . $this->faker()->randomNumber(4)));
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
        $this->expectException(\RuntimeException::class);
        $builder = new ProducerBuilder(['fake.host:1337'], $this->schemaRegistryUrl, Origin::VIDEOBLOCKS());
        $builder->setTimeoutMs(10);
        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }

    public function testWithRedis()
    {
        $this->expectNotToPerformAssertions();
        $builder = new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS());
        $predis = new Client(['host' => $this->redisHost]);
        $builder->setPredisCache($predis);
        $producer = $builder->build();
        $producer->produce(FakeFactory::fakeRecord());
    }

    public function testInvalidSchema()
    {
        $producer = (new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS()))->build();

        try {
            $producer->produce(FakeFactory::invalidRecord());
        } catch (AvroEncodingException $e) {
            // pass
        }

        $invalidRecord = null;
        $consumer = (new ConsumerBuilder($this->brokerHosts, 'test-consumer', $this->schemaRegistryUrl, Origin::VIDEOBLOCKS()))->build();
        $consumer->subscribe(FailedRecord::class, function ($record) use ($consumer, &$invalidRecord) {
            $invalidRecord = $record;
            $consumer->disconnect();
        });
        $consumer->consume('invalid-fake-record');

        $this->assertNotNull($invalidRecord);
    }
}