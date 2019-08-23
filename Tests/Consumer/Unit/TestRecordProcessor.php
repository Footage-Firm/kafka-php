<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Consumer\RecordProcessor;
use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatting;
use AvroSchema;
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

    use WithFaker, RecordFormatting;

    private $mockRegistry;

    /** @var \Mockery\Mock|KafkaSerializerInterface */
    private $mockSerializer;

    private $mockFailureProducer;

    private $groupId;

    private $recordProcessor;

    private $mockSuccessFn;

    private $mockFailureFn;

    private $fakeRecord;

    private $mockMessage;

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();
        $this->mockRegistry = Mockery::mock(Registry::class);
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class)->shouldIgnoreMissing();
        $this->mockFailureProducer = Mockery::mock(Producer::class);
        $this->groupId = $this->faker->word;
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
        $this->fakeRecord = FakeFactory::fakeRecord();
        $this->mockMessage = Mockery::mock(Message::class);
        $this->recordProcessor = new RecordProcessor(
          $this->mockRegistry,
          $this->mockSerializer,
          $this->groupId,
          $this->mockFailureProducer
        );
    }

    public function testSubscribeAddsHandler()
    {
        $schemaId = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $this->recordProcessor->subscribe($this->fakeRecord, $this->mockSuccessFn, $this->mockFailureFn);

        $handlers = $this->recordProcessor->getHandlers();
        self::assertCount(1, $handlers);

        $handler = array_pop($handlers);
        self::assertSame($this->fakeRecord, $handler->getRecord());
    }

    public function testProcessCallsCorrectSuccessFunction()
    {
        $this->expectNotToPerformAssertions();

        $schemaId = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $fakeRecordTwo = FakeFactory::fakeRecordTwo();
        $mockSuccessFnTwo = new MockeryCallableMock();
        $mockFailureFnTwo = new MockeryCallableMock();
        $schemaIdTwo = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaIdTwo);

        $this->mockMessage->payload = valueOf(encode(1, $schemaId, json_encode($this->fakeRecord)));

        $this->mockSerializer->shouldReceive('deserialize')->andReturn($this->fakeRecord);

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

        $schemaId = $this->faker->randomNumber(1);

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $this->mockMessage->payload = valueOf(
          encode(1, $this->faker->randomNumber(2), json_encode($this->fakeRecord))
        );

        $this->recordProcessor->subscribe($fakeRecordTwo, $this->mockSuccessFn, $this->mockFailureFn);
        $this->recordProcessor->process($this->mockMessage);

        $this->mockSuccessFn->shouldNotHaveBeenCalled();
        $this->mockFailureFn->shouldNotHaveBeenCalled();
    }

    //    public function testDoNotSendToFailureTopic_WhenOptionIsDisabled()
    //    {
    //        $this->mockSuccessFn = new MockeryCallableMock();
    //        $this->mockSuccessFn->shouldBeCalled()->andThrow(Exception::class);
    //        $this->mockFailureFn = new MockeryCallableMock();
    //        $this->recordProcessor->setShouldSendToFailureTopic(false);
    //
    //    }

}