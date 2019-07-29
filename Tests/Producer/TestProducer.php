<?php

namespace Tests\Producer;

use App\Producer\Exceptions\ProducerException;
use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use Mockery;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use Tests\Fakes\FakeRecordFactory;
use Tests\TestCaseWithFaker;

class TestProducer extends TestCaseWithFaker
{

    private $mockKafkaProducer;

    private $mockSerializer;

    private $logger;

    public function setUp(): void
    {
        parent::setUp();

        $this->mockKafkaProducer = Mockery::mock(KafkaProducer::class);
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class);
        $this->logger = Mockery::mock(LoggerInterface::class);// new FakeLogger();
    }

    public function testOnlyOneTypeOfRecordProducedAtOnce()
    {

        $this->expectException(ProducerException::class);
        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->logger);
        $producer->produce([FakeRecordFactory::fakeRecord(), FakeRecordFactory::fakeRecordTwo()]);
    }
}