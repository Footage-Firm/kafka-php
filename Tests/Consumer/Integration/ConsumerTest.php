<?php

namespace Test\Consumer\Integration;

use EventsPhp\Storyblocks\Common\DebugRecord;
use EventsPhp\Storyblocks\Common\Origin;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Producer\ProducerBuilder;
use Tests\BaseTestCase;

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
        $producer->produce($record, null, $topic, false);

        $processed = false;

        $consumer->subscribe(DebugRecord::class, function (DebugRecord $record) use (&$consumer, &$processed) {
           $processed = true;
           $consumer->disconnect();
        });

        $consumer->consume([$topic]);

        $this->assertTrue($processed);

    }

}