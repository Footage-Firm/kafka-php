<?php


namespace Tests\Consumer\Integration;

use EventsPhp\Storyblocks\Common\DebugRecord;
use EventsPhp\Storyblocks\Common\Origin;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Logger\Logger;
use KafkaPhp\Producer\ProducerBuilder;
use Tests\BaseTestCase;
use Tests\Utils\Factory;

class ConsumerFailureTest extends BaseTestCase
{

    private $groupId;
    private $topic;

    public function setUp(): void
    {
        parent::setUp();
        $this->groupId = $this->faker()->word;
        $this->topic = 'test-'.$this->faker()->word;
    }

    public function testConsumerWritesToFailureTopic(): void
    {
        $this->expectNotToPerformAssertions();

        $consumer = $this->consumer();
        $producer = $this->producer();

        $consumer->subscribe(DebugRecord::class, function($record) {
                throw new \Exception('Intentional failure in '.__METHOD__);
            }, function() use ($consumer) {
                $consumer->disconnect();
            });

        $producer->produce(Factory::debugRecord($this->faker()->word), null, $this->topic);

        $consumer->consume($this->topic);
        $consumer->wait();
    }

    private function consumer() {
        $builder = (new ConsumerBuilder($this->brokerHosts, $this->groupId, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS(), new Logger()))
            ->setNumRetries(0);

        if ($this->saslUsername) {
            $builder->setSaslData($this->saslUsername, $this->saslPassword);
        }

        return $builder->build();
    }

    private function producer() {
        $builder = new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl, Origin::VIDEOBLOCKS());

        if ($this->saslUsername) {
            $builder->setSaslData($this->saslUsername, $this->saslPassword);
        }

        return $builder->build();
    }
}