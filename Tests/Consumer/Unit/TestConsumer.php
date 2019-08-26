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

    private $recordProcessor;

    private $logger;

    private $kafkaClient;

    private $mockRecord;

    private $mockSuccessFn;

    private $mockFailureFn;

    private $consumer;

    private $mockMessageHandler;

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();

        $this->recordProcessor = Mockery::mock(RecordProcessor::class);
        $this->logger = Mockery::mock(LoggerInterface::class);
        $this->mockRecord = Mockery::mock(BaseRecord::class);
        $this->mockMessageHandler = Mockery::mock(MessageHandler::class);
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->kafkaClient = Mockery::mock(KafkaConsumer::class);
        $this->kafkaClient->shouldReceive('unsubscribe');


        $this->consumer = new Consumer($this->kafkaClient, $this->logger, $this->recordProcessor);

    }

    public function testSubscribe()
    {
        $this->expectNotToPerformAssertions();

        $this->recordProcessor
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

        $this->kafkaClient->shouldReceive('subscribe')->once()->with($expectedTopic);
        $this->kafkaClient->shouldReceive('consume');

        $this->recordProcessor->shouldReceive('subscribe');
        $this->recordProcessor->shouldReceive('getHandlers')->andReturn([$this->mockMessageHandler]);

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
        $this->kafkaClient->shouldReceive('consume')->andReturn($mockMessage);
        $this->recordProcessor->shouldReceive('process')->once()->with($mockMessage);
    }
}