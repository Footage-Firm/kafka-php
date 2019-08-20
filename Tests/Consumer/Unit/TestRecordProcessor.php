<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Consumer\RecordProcessor;
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

    private $mockSerializer;

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();
        $this->mockRegistry = Mockery::mock(Registry::class);
        $this->mockSerializer = Mockery::mock(KafkaSerializerInterface::class);
    }

    public function testSubscribeAddsHandler()
    {
        $schemaId = $this->faker->randomDigitNotNull;
        $mockSuccess = new MockeryCallableMock();
        $mockFailure = new MockeryCallableMock();
        $fakeRecord = FakeFactory::fakeRecord();

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $recordProcessor = new RecordProcessor($this->mockRegistry, $this->mockSerializer);
        $recordProcessor->subscribe($fakeRecord, $mockSuccess, $mockFailure);

        $handlers = $recordProcessor->getHandlers();
        self::assertCount(1, $handlers);

        $handler = array_pop($handlers);
        self::assertSame($fakeRecord, $handler->getRecord());
    }

    public function testProcessCallsCorrectSuccessFunction()
    {
        $this->expectNotToPerformAssertions();

        $fakeRecord = FakeFactory::fakeRecord();
        $mockSuccess = new MockeryCallableMock();
        $mockFailure = new MockeryCallableMock();
        $schemaId = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $fakeRecordTwo = FakeFactory::fakeRecordTwo();
        $mockSuccessTwo = new MockeryCallableMock();
        $mockFailureTwo = new MockeryCallableMock();
        $schemaIdTwo = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaIdTwo);

        $recordProcessor = new RecordProcessor($this->mockRegistry, $this->mockSerializer);

        $recordProcessor->subscribe($fakeRecord, $mockSuccess, $mockFailure);
        $recordProcessor->subscribe($fakeRecordTwo, $mockSuccessTwo, $mockFailureTwo);

        $mockMessage = Mockery::mock(Message::class);
        $mockMessage->payload = valueOf(encode(1, $schemaId, json_encode($fakeRecord)));

        $this->mockSerializer->shouldReceive('deserialize')->andReturn($fakeRecord);
        $recordProcessor->process($mockMessage);

        $mockSuccess->shouldBeCalled()->once();
        $mockFailure->shouldNotHaveBeenCalled();
        $mockSuccessTwo->shouldNotHaveBeenCalled();
        $mockFailureTwo->shouldNotHaveBeenCalled();
    }

    public function testNoSuccessOrFailureCalledWhenNoHandlerIsRegisteredForClass()
    {
        $this->expectNotToPerformAssertions();

        $fakeRecordTwo = FakeFactory::fakeRecordTwo();
        $mockSuccess = new MockeryCallableMock();
        $mockFailure = new MockeryCallableMock();
        $schemaId = $this->faker->randomDigitNotNull;

        $this->mockRegistry
          ->shouldReceive('schemaId')
          ->with('fake-record-two-value', Mockery::type(AvroSchema::class))
          ->andReturn($schemaId);

        $recordProcessor = new RecordProcessor($this->mockRegistry, $this->mockSerializer);

        $recordProcessor->subscribe($fakeRecordTwo, $mockSuccess, $mockFailure);

        $mockMessage = Mockery::mock(Message::class);

        $mockMessage->payload = valueOf(
          encode(1, $this->faker->randomDigitNotNull, json_encode(FakeFactory::fakeRecordTwo()))
        );
        $recordProcessor->process($mockMessage);

        $mockSuccess->shouldNotHaveBeenCalled();
        $mockFailure->shouldNotHaveBeenCalled();
    }

}