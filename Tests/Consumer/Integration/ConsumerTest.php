<?php

namespace Test\Consumer\Integration;

use Akamon\MockeryCallableMock\MockeryCallableMock;
use EventsPhp\Storyblocks\Common\DebugRecord;
use EventsPhp\Storyblocks\Common\Origin;
use EventsPhp\Util\EventFactory;
use KafkaPhp\Consumer\Consumer;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Consumer\RecordHandler;
use KafkaPhp\Consumer\RecordProcessor;
use KafkaPhp\Producer\ProducerBuilder;
use KafkaPhp\Serializers\KafkaSerializerInterface;
use Mockery;
use PHPUnit\Framework\TestCase;
use Psr\Log\LoggerInterface;
use RdKafka\KafkaConsumer;
use RdKafka\Message;
use Tests\BaseTestCase;
use Tests\Util\Fakes\FakeRecord;

class ConsumerTest extends BaseTestCase
{

    public function testConsume()
    {

        $topic = $this->faker()->word;

        $consumer = (new ConsumerBuilder($this->brokerHosts, $this->faker()->word, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS()))
            ->setNumRetries(0)
            ->build();

        $producer = (new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS()))
            ->setNumRetries(0)
            ->build();

        $record = new DebugRecord();
        $record->setPayload("testConsume");
        $producer->produce($record, $topic, false);

        $processed = false;

        $consumer->subscribe(DebugRecord::class, function (DebugRecord $record) use (&$consumer, &$processed) {
           $processed = true;
           $consumer->disconnect();
        });

        $consumer->consume([$topic]);

        $this->assertTrue($processed);

    }

}