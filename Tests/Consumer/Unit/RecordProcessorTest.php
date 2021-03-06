<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use Exception;
use Hamcrest\Core\IsEqual;
use KafkaPhp\Common\TopicFormatter;
use KafkaPhp\Consumer\RecordProcessor;
use KafkaPhp\Logger\Logger;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use KafkaPhp\Traits\RecordFormatter;
use KafkaPhp\Traits\ShortClassName;
use Mockery;
use Tests\BaseTestCase;
use Tests\Utils\Fakes\FakeFactory;
use Tests\Utils\Fakes\FakeRecord;

class RecordProcessorTest extends BaseTestCase
{

    use RecordFormatter, ShortClassName;

    private $mockRegistry;

    /** @var \Mockery\Mock|KafkaSerializerInterface */
    private $mockSerializer;

    /** @var Producer|Mockery\Mock */
    private $mockFailureProducer;

    private $fakeGroupId;

    /** @var RecordProcessor */
    private $recordProcessor;

    private $mockSuccessFn;

    private $mockFailureFn;

    private $fakeRecord;

    private $mockMessage;

    private $fakeSchemaId;

    private $fakeDecodedArray;

    public function setUp(): void
    {
        $this->fakeRecord = FakeFactory::fakeRecord();
        $this->fakeDecodedArray = [
          self::shortClassName(FakeRecord::class) => $this->fakeRecord->jsonSerialize(),
        ];
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->mockFailureProducer = Mockery::mock(Producer::class);
        $this->fakeGroupId = $this->faker()->word;

        $this->initRecordProcessor();
    }

    protected function tearDown(): void
    {
        Mockery::close();
    }

    private function initRecordProcessor(): void
    {
        $this->recordProcessor = new RecordProcessor(
          $this->fakeGroupId,
          $this->mockFailureProducer,
          new Logger()
        );
    }

    public function testSubscribeAddsHandler(): void
    {
        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);

        $handlers = $this->recordProcessor->getHandlers();
        self::assertCount(1, $handlers);

        /** @var \KafkaPhp\Consumer\RecordHandler $handler */
        $handler = array_pop($handlers);
        self::assertSame(FakeRecord::class, $handler->getRecordType());
    }

    public function testProcessCallsCorrectSuccessFunction()
    {
        $this->expectNotToPerformAssertions();

        $mockSuccessFnTwo = new MockeryCallableMock();
        $mockFailureFnTwo = new MockeryCallableMock();

        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->subscribe(FakeRecordTwo::class, $mockSuccessFnTwo, $mockFailureFnTwo);
        $this->mockSuccessFn->shouldBeCalled()->once();

        $this->recordProcessor->process($this->fakeDecodedArray);

        $this->mockFailureFn->shouldNotHaveBeenCalled();
        $mockSuccessFnTwo->shouldNotHaveBeenCalled();
        $mockFailureFnTwo->shouldNotHaveBeenCalled();
    }

    public function testNoSuccessOrFailureCalled_WhenNoHandlerIsRegisteredForMessageClass()
    {
        $this->expectNotToPerformAssertions();

        $this->recordProcessor->subscribe(FakeRecordTwo::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->fakeDecodedArray);

        $this->mockSuccessFn->shouldNotHaveBeenCalled();
        $this->mockFailureFn->shouldNotHaveBeenCalled();
    }

    public function testDoNotSendToFailureTopic_WhenOptionIsDisabled()
    {
        $this->expectNotToPerformAssertions();

        $this->mockSuccessFn->shouldBeCalled()->andThrow(Exception::class);

        $this->recordProcessor->setShouldSendToFailureTopic(false);
        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->fakeDecodedArray);

        $this->mockFailureProducer->shouldNotHaveBeenCalled();
    }

    public function testCorrectNumberOfTriesAreRun()
    {
        $this->expectNotToPerformAssertions();

        $numRetries = $this->faker()->randomNumber(1);
        $topic = sprintf('%s%s-%s', TopicFormatter::FAILURE_TOPIC_PREFIX, $this->fakeGroupId, 'fake-record');
        $this->mockFailureProducer->shouldReceive('produce')
          ->once()
          ->with(
            IsEqual::equalTo($this->fakeRecord),
            null,
            $topic
          );

        $this->initRecordProcessor();
        $this->mockSuccessFn->shouldBeCalled()->times($numRetries + 1)->andThrow(Exception::class);

        $this->recordProcessor->setNumRetries($numRetries);
        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->fakeDecodedArray);
    }
}