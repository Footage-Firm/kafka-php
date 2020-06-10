<?php

namespace KafkaPhp\Examples;

use Dotenv\Dotenv;
use EventsPhp\Storyblocks\Billing\ChargebackEvent;
use EventsPhp\Storyblocks\Common\Origin;
use EventsPhp\Storyblocks\Telemetry\TelemetryEvent;
use KafkaPhp\Consumer\ConsumerBuilder;
use Monolog\Logger;

require_once __DIR__ . '/../../vendor/autoload.php';

class ConsumerExample
{

    private $schemaRegistryUrl;
    private $brokerHosts;
    private $saslUsername;
    private $saslPassword;

    public function __construct()
    {
        $ROOT = __DIR__ . '/../../';
        $dotenv = Dotenv::create($ROOT);
        $dotenv->load();
        $dotenv->required(['BROKER_HOSTS', 'SCHEMA_REGISTRY_URL', 'BROKER_SASL_USERNAME', 'BROKER_SASL_PASSWORD']);
        $this->brokerHosts = preg_split('/,/', getenv('BROKER_HOSTS'));
        $this->saslUsername = getenv('BROKER_SASL_USERNAME');
        $this->saslPassword = getenv('BROKER_SASL_PASSWORD');
        $this->schemaRegistryUrl = getenv('SCHEMA_REGISTRY_URL');
        $this->env = getenv('ENV');
    }

    public function simpleConsume(): void
    {
        $builder = new ConsumerBuilder(
            $this->brokerHosts,
            'example-consumer-local-2',
            $this->schemaRegistryUrl,
            Origin::OUTDATED_SCHEMA(),
            new \KafkaPhp\Logger\Logger()
        );

        $consumer = $builder
            ->setSaslData($this->saslUsername, $this->saslPassword)
            ->shouldSendToFailureTopic(false)
            ->setNumRetries(0)
            ->setOffsetReset('end')
            ->build();

        $consumer->subscribe(TelemetryEvent::class, function (TelemetryEvent $event) {
            print 'Telemetry event: ' . $event->getMeta()->getUuid() . PHP_EOL;
        });

        $consumer->subscribe(ChargebackEvent::class, function (TelemetryEvent $event) {
            print 'Chargeback event: ' . $event->getMeta()->getUuid() . PHP_EOL;
        });

        print 'Starting consumer...' . PHP_EOL;
        $consumer->consume();
        $consumer->wait();
    }

    public function consumeFromBeginningAndEnd()
    {

        // This consumer will consume all existing records and all new records produced to 'fake-record'
        $consumerBeginning = (new ConsumerBuilder($this->brokers, 'beg', $this->schemaRegistryUrl))->build();
        $consumerBeginning->subscribe(FakeRecord::class, function ($record) {
            print '[consumerBeginning] id: ' . $record->getId() . PHP_EOL;
        });
        $consumerBeginning->consume();


        // This consumer will only consume records that are produced after it connects to kafka
        $consumerEnd = (new ConsumerBuilder($this->brokers, 'new', $this->schemaRegistryUrl))
            ->setOffsetReset('end')->build();
        $consumerEnd->subscribe(FakeRecord::class, function ($record) {
            print '[consumerEnd] id: ' . $record->getId() . PHP_EOL;
        });
        $consumerEnd->consume();

        $consumerEnd->wait();
        $consumerBeginning->wait();
    }
}

$ex = new ConsumerExample();
$ex->simpleConsume();