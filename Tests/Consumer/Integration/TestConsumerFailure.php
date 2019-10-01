<?php


namespace Tests\Consumer\Integration;

use EventsPhp\Storyblocks\Common\DebugRecord;
use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Logger\Logger;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Producer\ProducerBuilder;
use EventsPhp\BaseRecord;
use PHPUnit\Framework\TestCase;
use Tests\Fakes\FakeFactory;
use Tests\Utils\Factory;
use Tests\WithFaker;

class TestConsumerFailure extends TestCase
{

    use WithFaker;

    private $schemaRegistryUrl = 'http://0.0.0.0:8081';

    private $brokers = ['0.0.0.0:29092'];

    private $groupId;
    private $topic;
    private $env;

    public function setUp(): void
    {
        $this->initFaker();
        $this->groupId = $this->faker->word;
        $this->brokers = getenv('BROKER_HOST') ? [getenv('BROKER_HOST')] : $this->brokers;
        $this->schemaRegistryUrl = getenv('SCHEMA_REGISTRY_URL') ?: $this->schemaRegistryUrl;
        $this->topic = 'test-'.$this->faker->word;
        $this->env = getenv('ENV');
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

        $producer->produce(Factory::debugRecord($this->faker->word), $this->topic);

        $consumer->consume($this->topic);
        $consumer->wait();
    }

    private function consumer() {
        $builder = (new ConsumerBuilder($this->brokers, $this->groupId, $this->schemaRegistryUrl, new Logger()))
            ->setNumRetries(0);

        if ($this->env) {
            $CERTS = __DIR__.'/../../../certs';
            $builder->setSslData(
                "$CERTS/$this->env/ca.pem",
                "$CERTS/$this->env/service.cert",
                "$CERTS/$this->env/service.key"
            );
        }

        return $builder->build();
    }

    private function producer() {
        $builder = new ProducerBuilder($this->brokers, $this->schemaRegistryUrl);

        if ($this->env) {
            $CERTS = __DIR__.'/../../../certs';
            $builder->setSslData(
                "$CERTS/$this->env/ca.pem",
                "$CERTS/$this->env/service.cert",
                "$CERTS/$this->env/service.key"
            );
        }

        return $builder->build();
    }
}