<?php

namespace Tests\Producer\Unit;

use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use Mockery;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use RdKafka\ProducerTopic;
use Tests\Fakes\FakeRecordFactory;
use Tests\TestCaseWithFaker;
use Throwable;

class TestProducer extends TestCaseWithFaker
{

    /** @var \Mockery\Mock|\RdKafka\Producer */
    private $mockKafkaProducer;

    private $mockSerializer;

    private $mockLogger;

    private $topic;

    /** @var \Tests\Fakes\FakeRecord */
    private $mockRecord;

    private $fakeEncodedRecord;

    public function setUp(): void
    {
        parent::setUp();

        $this->topic = $this->faker->word;
        $this->mockKafkaProducer = Mockery::mock(KafkaProducer::class)->shouldIgnoreMissing();
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class);
        $this->mockLogger = Mockery::mock(LoggerInterface::class);// new FakeLogger();
        $this->mockRecord = FakeRecordFactory::fakeRecord();
        $this->mockEncodedRecord = $this->faker->word;
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testProducesCorrectRecord()
    {
        $this->mockSerializer->shouldReceive('serialize')->andReturn($this->mockEncodedRecord);

        $mockTopicProducer = Mockery::mock(ProducerTopic::class);
        $mockTopicProducer->shouldReceive('produce')->withArgs([RD_KAFKA_PARTITION_UA, 0, $this->mockEncodedRecord]);

        $this->mockKafkaProducer->shouldReceive('newTopic')->andReturn($mockTopicProducer);

        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);
        $producer->produce($this->mockRecord, $this->topic);
    }

    public function testExceptionThrownIfEncodingError()
    {
        $this->mockSerializer->shouldReceive('serialize')->andThrow(Throwable::class);

    }

    //    public function testFailureRecordIsProduce_WhenOptionIsTrue()
    //    {
    //
    //        $this->mockSerializer->shouldReceive('serialize')->andThrow(Throwable::class);
    //    }


}