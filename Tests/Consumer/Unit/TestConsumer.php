<?php

namespace Test\Consumer\Unit;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use App\Consumer\Consumer;
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

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();

        $this->recordProcessor = Mockery::mock(RecordProcessor::class);
        $this->logger = Mockery::mock(LoggerInterface::class);
        $this->kafkaClient = Mockery::mock(KafkaConsumer::class);
        $this->mockRecord = Mockery::mock(BaseRecord::class);
        $this->mockSuccessFn = new MockeryCallableMock();
        $this->mockFailureFn = new MockeryCallableMock();
    }

    public function testSubscribe()
    {
        $this->expectNotToPerformAssertions();
        
        $this->recordProcessor
          ->shouldReceive('subscribe')
          ->with($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);

        $consumer = new Consumer($this->kafkaClient, $this->logger, $this->recordProcessor);
        $consumer->subscribe($this->mockRecord, $this->mockSuccessFn, $this->mockFailureFn);
    }
}