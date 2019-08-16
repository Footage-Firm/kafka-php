<?php

namespace Tests\Producer\Unit;

use App\Producer\Producer;
use App\Producer\Producer as ProducerAlias;
use App\Serializers\KafkaSerializerInterface;
use Exception;
use Mockery;
use Psr\Log\LoggerInterface;
use RdKafka\Producer as KafkaProducer;
use RdKafka\ProducerTopic;
use Tests\Fakes\FakeRecord;
use Tests\Fakes\FakeRecordFactory;
use Tests\TestCaseWithFaker;

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
        $this->fakeEncodedRecord = $this->faker->word;
    }

    /**
     * @doesNotPerformAssertions
     */
    public function testProducesCorrectRecord()
    {
        $this->mockSerializer->shouldReceive('serialize')->andReturn($this->fakeEncodedRecord);

        $mockTopicProducer = Mockery::mock(ProducerTopic::class);
        $mockTopicProducer->shouldReceive('produce')->withArgs([RD_KAFKA_PARTITION_UA, 0, $this->fakeEncodedRecord]);
        $this->mockKafkaProducer->shouldReceive('newTopic')->andReturn($mockTopicProducer);

        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);
        $producer->produce($this->mockRecord, $this->topic);
    }


    public function testExceptionThrownAndFailureProducedWhenInitialProductionFails()
    {
        $this->expectException(Exception::class);

        $this->mockSerializer->shouldReceive('serialize')->andReturn($this->fakeEncodedRecord);
        $this->mockLogger->shouldReceive('error');

        // Throw an error when trying to produce initially
        $mockTopicProducer_Fail = Mockery::mock(ProducerTopic::class);
        $mockTopicProducer_Fail->shouldReceive('produce')
          ->withArgs([RD_KAFKA_PARTITION_UA, 0, $this->fakeEncodedRecord])
          ->times(1)
          ->andThrow(Exception::class);

        // Don't throw an error when producing the failure record
        $mockTopicProducer_Success = Mockery::mock(ProducerTopic::class)->shouldIgnoreMissing();
        $mockTopicProducer_Success->shouldReceive('produce')
          ->times(1)
          ->withArgs([
            RD_KAFKA_PARTITION_UA,
            0,
            $this->fakeEncodedRecord,
          ]);

        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with('fake-record')
          ->andReturn($mockTopicProducer_Fail);

        $this->mockKafkaProducer->shouldReceive('newTopic')
          ->with('fail-FakeRecord')
          ->andReturn($mockTopicProducer_Success);

        /** @var ProducerAlias $producer */
        $producer = new Producer($this->mockKafkaProducer, $this->mockSerializer, $this->mockLogger);

        $producer->produce(new FakeRecord());
    }

}