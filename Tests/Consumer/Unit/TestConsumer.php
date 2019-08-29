<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Consumer\Consumer;
use App\Consumer\MessageHandler;
use App\Consumer\RecordProcessor;
use App\Events\BaseRecord;
use Mockery;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use Tests\WithFaker;

class TestConsumer extends TestCase
{

    use WithFaker;

    private $mockRecordProcessor;

    private $logger;

    private $mockDafkaClient;

    private $mockRecord;

    private $mockSuccessFn;

    private $mockFailureFn;

    private $mockMessageHandler;

    private $consumer;

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();

        $this->mockRecordProcessor = Mockery::mock(RecordProcessor::class);
        $this->logger = Mockery::mock(LoggerInterface::class);
        $this->mockRecord = Mockery::mock(BaseRecord::class);
        $this->mockMessageHandler = Mockery::mock(MessageHandler::class);
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->mockDafkaClient = Mockery::mock(KafkaConsumer::class);
        $this->mockDafkaClient->shouldReceive('unsubscribe');


        $this->consumer = new Consumer($this->mockDafkaClient, $this->logger, $this->mockRecordProcessor);

    }

    public function testSubscribe()
    {
        $this->expectNotToPerformAssertions();

        $this->mockRecordProcessor
          ->shouldReceive('subscribe')
          ->with($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);

        $this->consumer->subscribe($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);
    }

    /**
     * @dataProvider topicDataProvider
     */
    public function testCorrectTopicNameUsed($originalTopic, $expectedTopic)
    {
        $this->expectNotToPerformAssertions();

        $this->mockDafkaClient->shouldReceive('subscribe')->once()->with($expectedTopic);
        $this->mockDafkaClient->shouldReceive('consume');

        $this->mockRecordProcessor->shouldReceive('subscribe');
        $this->mockRecordProcessor->shouldReceive('getHandlers')->andReturn([$this->mockMessageHandler]);

        $this->consumer->subscribe($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);
        $this->consumer->setConsumerLifetime(0);
        $this->consumer->consume($originalTopic);
    }

    public function topicDataProvider()
    {
        $this->initFaker();
        $fakeTopics = $this->faker->words(3);

        return [
          [null, ['fake-record']],
          [$fakeTopics[0], [$fakeTopics[0]]],
          [$fakeTopics, $fakeTopics],
        ];
    }

    public function testProcessesMessageFromKafka()
    {
        $this->expectNotToPerformAssertions();
        $mockMessage = Mockery::mock(Message::class);
        $this->mockDafkaClient->shouldReceive('consume')->andReturn($mockMessage);
        $this->mockRecordProcessor->shouldReceive('process')->once()->with($mockMessage);
    }

    public function testConsumerDiesAfterLifetimeIsUp()
    {
        $this->mockDafkaClient->shouldIgnoreMissing();

        $this->mockRecordProcessor->shouldIgnoreMissing();
        $this->mockRecordProcessor->shouldReceive('getHandlers')->andReturn([$this->mockMessageHandler]);

        $this->consumer->subscribe($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);
        $this->consumer->setConsumerLifetime(0);
        $this->consumer->consume();
        
        // If the consumer lifetime didn't work we'll never reach this assertion.
        $this->assertTrue(true);
    }
}