<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use KafkaPhp\Consumer\Consumer;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Consumer\RecordHandler;
use KafkaPhp\Consumer\RecordProcessor;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use Mockery;
use Mockery\Mock;
use Psr\Log\LoggerInterface;
use RdKafka\Exception;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Tests\BaseTestCase;
use Tests\Utils\Fakes\FakeRecord;

class ConsumerTest extends BaseTestCase
{

    /** @var Mock|RecordProcessor */
    private $mockRecordProcessor;

    /** @var Mock|LoggerInterface */
    private $mockLogger;

    /** @var Mock|KafkaConsumer*/
    private $mockKafkaClient;

    /** @var Mock|callable */
    private $mockSuccessFn;

    /** @var Mock|callable */
    private $mockFailureFn;

    /** @var Mock|callable */
    private $mockMessageHandler;

    /** @var Mock|KafkaSerializerInterface */
    private $mockSerializer;

    /** @var Consumer */
    private $consumer;

    public function setUp(): void
    {
        parent::setUp();
        $this->setUpmocks();

        $this->consumer = new Consumer(
          $this->mockKafkaClient,
          $this->mockSerializer,
          $this->mockLogger,
          $this->mockRecordProcessor,
          1
        );

    }

    protected function tearDown(): void
    {
        Mockery::close();
    }

    public function testSubscribe()
    {
        $this->expectNotToPerformAssertions();

        $this->mockRecordProcessor
          ->shouldReceive('subscribe')
          ->with(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);

        $this->consumer
            ->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn)
            ->wait();

    }

    /**
     * @dataProvider topicDataProvider
     */
    public function testCorrectTopicNameUsed($originalTopic, $expectedTopic)
    {
        $this->expectNotToPerformAssertions();

        $this->mockKafkaClient->shouldReceive('subscribe')->once()->with($expectedTopic);
        $this->mockKafkaClient->shouldReceive('consume')->with(ConsumerBuilder::DEFAULT_TIMEOUT_MS);

        $this->mockRecordProcessor->shouldReceive('subscribe');
        $this->mockRecordProcessor->shouldReceive('getHandlers')
          ->andReturn([FakeRecord::class => $this->mockMessageHandler]);

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->consumer->consume($originalTopic)->wait();

    }

    public function topicDataProvider()
    {
        $fakeTopics = $this->faker()->words(3);
        return [
          [null, ['fake-record']],
          [$fakeTopics[0], [$fakeTopics[0]]],
          [$fakeTopics, $fakeTopics],
        ];
    }

    public function testConsumerDiesAfterLifetimeIsUp()
    {
        $this->mockKafkaClient->shouldIgnoreMissing();

        $this->mockRecordProcessor->shouldIgnoreMissing();
        $this->mockRecordProcessor->shouldReceive('getHandlers')->andReturn([$this->mockMessageHandler]);

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->consumer->consume(null)->wait();

        // If the consumer lifetime didn't work we'll never reach this assertion.
        $this->assertTrue(true);
    }

    public function testHandlesMessageFromKafka_NoError()
    {
        $this->expectNotToPerformAssertions();
        $this->mockWorkingConsumer();

        $this->mockKafkaClient->shouldReceive('commit');
        $this->mockLogger->shouldNotHaveBeenCalled();

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn);
        $this->consumer->consume()->wait();
    }

    public function testHandlesMessageFromKafka_TimeoutError()
    {
        $this->expectNotToPerformAssertions();
        $this->mockKafkaClientWithMessage(RD_KAFKA_RESP_ERR__TIMED_OUT);

        $this->mockSerializer->shouldNotReceive('deserialize');

        $this->mockRecordProcessor->shouldReceive('subscribe')
          ->with(FakeRecord::class, $this->mockSuccessFn, null);
        $this->mockRecordProcessor->shouldNotReceive('process');
        $this->mockRecordProcessor->shouldReceive('getHandlers')
          ->andReturn([FakeRecord::class => $this->mockMessageHandler]);

        $this->mockLogger->shouldNotHaveBeenCalled();

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn);
        $this->consumer->consume()->wait();

    }

    public function testHandlesMessageFromKafka_NonTimeoutError()
    {
        $this->expectNotToPerformAssertions();

        // A random selection of possible errors
        $someErrors = [
          RD_KAFKA_RESP_ERR__TRANSPORT,
          RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
          RD_KAFKA_RESP_ERR__RESOLVE,
          RD_KAFKA_RESP_ERR__MSG_TIMED_OUT,
          RD_KAFKA_RESP_ERR__PARTITION_EOF,
          RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION,
          RD_KAFKA_RESP_ERR__FS,
          RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC,
          RD_KAFKA_RESP_ERR__ALL_BROKERS_DOWN,
          RD_KAFKA_RESP_ERR__INVALID_ARG,
          RD_KAFKA_RESP_ERR__QUEUE_FULL,
          RD_KAFKA_RESP_ERR__ISR_INSUFF,
          RD_KAFKA_RESP_ERR__NODE_UPDATE,
          RD_KAFKA_RESP_ERR__SSL,
        ];
        $err = array_rand($someErrors);
        $errStr = $this->faker()->word;
        $this->mockKafkaClientWithMessage($err, $errStr);

        $this->mockSerializer->shouldNotReceive('deserialize');

        $this->mockRecordProcessor->shouldReceive('subscribe')
          ->with(FakeRecord::class, $this->mockSuccessFn, null);
        $this->mockRecordProcessor->shouldNotReceive('process');
        $this->mockRecordProcessor->shouldReceive('getHandlers')
          ->andReturn([FakeRecord::class => $this->mockMessageHandler]);

        $this->mockLogger->shouldReceive('info')->with('Kafka message error: ' . $errStr);

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn);
        $this->consumer->consume()->wait();

    }

    public function testRetryCommitOnTimeout()
    {
        $this->expectNotToPerformAssertions();

        $this->mockWorkingConsumer();

        $this->mockKafkaClient->shouldReceive('commit')
            ->once()
            ->andThrow(new Exception('Request timed out'))
            ->once()
            ->andReturn();

        $this->consumer->subscribe(FakeRecord::class, $this->mockSuccessFn);
        $this->consumer->consume()->wait();

    }

    private function mockWorkingConsumer() {
        $fakePayload = "fake";
        $fakeDeserialized = [];
        $this->mockKafkaClientWithMessage(RD_KAFKA_RESP_ERR_NO_ERROR, '', $fakePayload);
        $this->mockSerializer->shouldReceive('deserialize')->with($fakePayload)->andReturn($fakeDeserialized);

        $this->mockRecordProcessor->shouldReceive('subscribe')->with(FakeRecord::class, $this->mockSuccessFn, null);
        $this->mockRecordProcessor->shouldReceive('process')->times(1)->with($fakeDeserialized);
        $this->mockRecordProcessor->shouldReceive('getHandlers')
            ->andReturn([FakeRecord::class => $this->mockMessageHandler]);
    }

    private function mockKafkaClientWithMessage($msgErr, $errStr = '', $msgPayload = '', $topic = 'fake-record')
    {

        $mockMessage = Mockery::mock(Message::class);
        if (!empty($errStr)) {
            $mockMessage->shouldReceive('errstr')->andReturn($errStr);
        }
        $mockMessage->err = $msgErr;
        $mockMessage->payload = $msgPayload;

        $this->mockKafkaClient->shouldReceive('consume')->andReturn($mockMessage);
        $this->mockKafkaClient->shouldReceive('subscribe')->with([$topic]);
    }

    private function setUpmocks()
    {
        $this->mockRecordProcessor = Mockery::mock(RecordProcessor::class)->shouldIgnoreMissing();
        $this->mockLogger = Mockery::mock(LoggerInterface::class)->shouldIgnoreMissing();
        $this->mockMessageHandler = Mockery::mock(RecordHandler::class)->shouldIgnoreMissing();
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->mockKafkaClient = Mockery::mock(KafkaConsumer::class)->shouldIgnoreMissing();
        $this->mockKafkaClient->shouldReceive('unsubscribe');
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class)->shouldIgnoreMissing();
    }
}