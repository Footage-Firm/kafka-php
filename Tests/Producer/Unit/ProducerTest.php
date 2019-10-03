<?php

namespace Tests\Producer\Unit;

use Error;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Producer\Producer as ProducerAlias;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use Mockery;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use RdKafka\ProducerTopic;
use Tests\BaseTestCase;
use Tests\Util\Fakes\FakeFactory;
use Tests\Util\Fakes\FakeRecord;

class ProducerTest extends BaseTestCase
{

    /** @var \Mockery\Mock|\RdKafka\Producer */
    private $mockKafkaProducer;

    private $mockSerializer;

    private $mockLogger;

    private $topic;

    /** @var FakeRecord */
    private $mockRecord;

    private $fakeEncodedRecord;

    /** @var ProducerTopic */
    private $mockTopicProducer;

    public function setUp(): void
    {
        parent::setUp();

        $this->topic = $this->faker()->word;
        $this->fakeEncodedRecord = $this->faker()->word;
        $this->mockKafkaProducer = Mockery::mock(KafkaProducer::class)->shouldIgnoreMissing();
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class);
        $this->mockLogger = Mockery::mock(LoggerInterface::class);// new FakeLogger();
        $this->mockRecord = FakeFactory::fakeRecord();
        $this->mockTopicProducer = Mockery::mock(ProducerTopic::class);

        $this->mockSerializer->shouldReceive('serialize')->andReturn($this->fakeEncodedRecord);
    }

    protected function tearDown(): void
    {
        Mockery::close();
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testProducesCorrectRecord()
    {
        $this->mockTopicProducer->shouldReceive('produce')->withArgs([
          RD_KAFKA_PARTITION_UA,
          0,
          $this->fakeEncodedRecord,
        ]);
        $this->mockKafkaProducer->shouldReceive('newTopic')->andReturn($this->mockTopicProducer);

        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);
        $producer->produce($this->mockRecord, $this->topic);
    }

    public function testCorrectTopicIsProduced_NoneProvided()
    {
        $this->expectNotToPerformAssertions();

        $this->mockTopicProducer->shouldReceive('produce')->withArgs([
          RD_KAFKA_PARTITION_UA,
          0,
          $this->fakeEncodedRecord,
        ]);
        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with('fake-record')
          ->andReturn($this->mockTopicProducer);
    }

    public function testCorrectTopicIsProduced_TopicProvided()
    {
        $fakeTopic = $this->faker()->word;

        $this->expectNotToPerformAssertions();

        $this->mockTopicProducer->shouldReceive('produce')->withArgs([
          RD_KAFKA_PARTITION_UA,
          0,
          $this->fakeEncodedRecord,
        ]);
        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with($fakeTopic)
          ->andReturn($this->mockTopicProducer);

        /** @var ProducerAlias $producer */
        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);

        $producer->produce(new FakeRecord(), $fakeTopic);
    }

    public function testExceptionThrownAndFailureProducedWhenInitialProductionFails()
    {
        $this->expectException(Error::class);

        $this->mockSerializer->shouldReceive('serialize')->andReturn($this->fakeEncodedRecord);
        $this->mockLogger->shouldReceive('error');

        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with('fake-record')
          ->andReturn($this->mockTopicProducer);

        // Throw an error when trying to produce initially
        $this->mockTopicProducer->shouldReceive('produce')
          ->withArgs([RD_KAFKA_PARTITION_UA, 0, $this->fakeEncodedRecord])
          ->times(1)
          ->andThrow(Error::class);

        // Don't throw an error when producing the failure record
        $mockTopicProducer_FailedRecord = Mockery::mock(ProducerTopic::class)->shouldIgnoreMissing();
        $mockTopicProducer_FailedRecord->shouldReceive('produce')
          ->times(1)
          ->withArgs([
            RD_KAFKA_PARTITION_UA,
            0,
            $this->fakeEncodedRecord,
          ]);
        
        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with('invalid-fake-record')
          ->andReturn($mockTopicProducer_FailedRecord);

        /** @var ProducerAlias $producer */
        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);

        $producer->produce(new FakeRecord());
    }

}