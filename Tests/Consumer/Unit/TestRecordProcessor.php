<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Common\TopicFormatter;
use App\Consumer\RecordProcessor;
use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Exception;
use Hamcrest\Core\IsEqual;
use Mockery;
use PHPUnit\Framework\TestCase;
use Tests\Fakes\FakeFactory;
use Tests\Fakes\FakeRecord;
use Tests\Fakes\FakeRecordTwo;
use Tests\WithFaker;


class TestRecordProcessor extends TestCase
{

    use WithFaker, RecordFormatter, ShortClassName;

    private $mockRegistry;

    /** @var \Mockery\Mock|KafkaSerializerInterface */
    private $mockSerializer;

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
        parent::setUp();

        $this->initFaker();


        $this->fakeRecord = FakeFactory::fakeRecord();
        $this->fakeDecodedArray = [
          self::shortClassName(FakeRecord::class) => $this->fakeRecord->jsonSerialize(),
        ];
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->mockFailureProducer = Mockery::mock(Producer::class);
        $this->fakeGroupId = $this->faker->word;

        $this->initRecordProcessor();
    }

    private function initRecordProcessor(): void
    {
        $this->recordProcessor = new RecordProcessor(
          $this->fakeGroupId,
          $this->mockFailureProducer
        );
    }

    public function testSubscribeAddsHandler(): void
    {
        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);

        $handlers = $this->recordProcessor->getHandlers();
        self::assertCount(1, $handlers);

        /** @var \App\Consumer\RecordHandler $handler */
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
        $this->recordProcessor->process($this->fakeDecodedArray);

        $this->mockSuccessFn->shouldBeCalled()->once();
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

        $numRetries = $this->faker->randomNumber(1);
        $topic = sprintf('%s%s-%s', TopicFormatter::FAILURE_TOPIC_PREFIX, $this->fakeGroupId, 'fake-record');
        $this->mockFailureProducer->shouldReceive('produce')
          ->times($numRetries + 1)
          ->with(
            IsEqual::equalTo($this->fakeRecord),
            $topic
          );

        $this->initRecordProcessor();
        $this->recordProcessor->setNumRetries($numRetries);

        $this->mockSuccessFn->shouldBeCalled()->times($numRetries + 1)->andThrow(Exception::class);

        $this->recordProcessor->setNumRetries($numRetries);
        $this->recordProcessor->subscribe(FakeRecord::class, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->fakeDecodedArray);
    }
}