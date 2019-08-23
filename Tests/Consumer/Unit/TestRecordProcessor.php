<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Common\TopicFormatter;
use App\Consumer\RecordProcessor;
use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use AvroSchema;
use Exception;
use FlixTech\SchemaRegistryApi\Registry;
use Mockery;
use PHPUnit\Framework\TestCase;
use RdKafka\Message;
use Tests\Fakes\FakeFactory;
use Tests\WithFaker;
use function FlixTech\AvroSerializer\Protocol\encode;
use function Widmogrod\Functional\valueOf;


class TestRecordProcessor extends TestCase
{

    use WithFaker, RecordFormatter;

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

    public function setUp(): void
    {
        parent::setUp();

        $this->initFaker();

        $this->fakeRecord = FakeFactory::fakeRecord();

        $this->initMockSchemaRegistry();
        $this->initMockSerializer();

        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();

        $this->mockFailureProducer = Mockery::mock(Producer::class);
        $this->fakeGroupId = $this->faker->word;
        $this->initRecordProcessor();
    }

    private function initMockSerializer(): void
    {
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class)->shouldIgnoreMissing();
        $this->mockSerializer->shouldReceive('deserialize')->andReturn($this->fakeRecord);
    }

    private function initMockSchemaRegistry(): void
    {
        $this->mockRegistry = Mockery::mock(Registry::class);
        $this->mockMessage = Mockery::mock(Message::class);
        $this->fakeSchemaId = $this->faker->randomNumber(1);
        $this->mockMessage->payload = valueOf(encode(1, $this->fakeSchemaId, json_encode($this->fakeRecord)));
        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-value', Mockery::type(AvroSchema::class))
          ->andReturn($this->fakeSchemaId);
    }

    private function initRecordProcessor()
    {
        $this->recordProcessor = new RecordProcessor(
          $this->mockRegistry,
          $this->mockSerializer,
          $this->fakeGroupId,
          $this->mockFailureProducer
        );
    }

    public function testSubscribeAddsHandler()
    {
        $this->recordProcessor->subscribe($this->fakeRecord, $this->mockSuccessFn, $this->mockFailureFn);

        $handlers = $this->recordProcessor->getHandlers();
        self::assertCount(1, $handlers);

        $handler = array_pop($handlers);
        self::assertSame($this->fakeRecord, $handler->getRecord());
    }

    public function testProcessCallsCorrectSuccessFunction()
    {
        $this->expectNotToPerformAssertions();

        $fakeRecordTwo = FakeFactory::fakeRecordTwo();
        $mockSuccessFnTwo = new MockeryCallableMock();
        $mockFailureFnTwo = new MockeryCallableMock();
        $schemaIdTwo = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaIdTwo);


        $this->recordProcessor->subscribe($this->fakeRecord, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->subscribe($fakeRecordTwo, $mockSuccessFnTwo, $mockFailureFnTwo);
        $this->recordProcessor->process($this->mockMessage);

        $this->mockSuccessFn->shouldBeCalled()->once();
        $this->mockFailureFn->shouldNotHaveBeenCalled();
        $mockSuccessFnTwo->shouldNotHaveBeenCalled();
        $mockFailureFnTwo->shouldNotHaveBeenCalled();
    }

    public function testNoSuccessOrFailureCalled_WhenNoHandlerIsRegisteredForMessageClass()
    {
        $this->expectNotToPerformAssertions();

        $fakeRecordTwo = FakeFactory::fakeRecordTwo();
        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($this->fakeSchemaId);

        $this->mockMessage->payload = valueOf(
          encode(1, $this->faker->randomNumber(2), json_encode($this->fakeRecord))
        );
        $this->recordProcessor->subscribe($fakeRecordTwo, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->mockMessage);

        $this->mockSuccessFn->shouldNotHaveBeenCalled();
        $this->mockFailureFn->shouldNotHaveBeenCalled();
    }

    public function testDoNotSendToFailureTopic_WhenOptionIsDisabled()
    {
        $this->expectNotToPerformAssertions();

        $this->mockSuccessFn->shouldBeCalled()->andThrow(Exception::class);

        $this->recordProcessor->setShouldSendToFailureTopic(false);
        $this->recordProcessor->subscribe($this->fakeRecord, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->mockMessage);

        $this->mockFailureProducer->shouldNotHaveBeenCalled();
    }

    public function testCorrectNumberOfTriesAreRun()
    {
        $this->expectNotToPerformAssertions();

        $numRetries = $this->faker->randomNumber(1);

        $this->mockFailureProducer->shouldReceive('produce')
          ->times($numRetries + 1)
          ->with(
            $this->fakeRecord,
            sprintf('%s%s-%s', TopicFormatter::FAILURE_TOPIC_PREFIX, $this->fakeGroupId, 'fake-record')
          );

        $this->initRecordProcessor();
        $this->recordProcessor->setNumRetries($numRetries);

        $this->mockSuccessFn->shouldBeCalled()->times($numRetries + 1)->andThrow(Exception::class);

        $this->recordProcessor->setNumRetries($numRetries);
        $this->recordProcessor->subscribe($this->fakeRecord, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->mockMessage);
    }


}