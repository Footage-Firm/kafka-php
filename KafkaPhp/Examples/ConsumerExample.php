<?php

namespace KafkaPhp\Example;

use KafkaPhp\Consumer\ConsumerBuilder;
use Tests\Fakes\FakeRecord;

require_once __DIR__ . '/../../vendor/autoload.php';

class ConsumerExample
{

    private $schemaRegistryUrl = 'http://0.0.0.0:8081';

    private $brokers = ['0.0.0.0:29092'];

    public function __construct()
    {
        $this->brokers = getenv('BROKER_HOSTS') ? [getenv('BROKER_HOSTS')] : $this->brokers;
        $this->schemaRegistryUrl = getenv('SCHEMA_REGISTRY_URL') ?: $this->schemaRegistryUrl;
    }

    public function simpleConsume(): void
    {
        $consumer = (new ConsumerBuilder($this->brokers, 'myGroupId', $this->schemaRegistryUrl))->build();
        $consumer->subscribe(FakeRecord::class, function ($record)
        {
            print 'Record consumer, id: ' . $record->getId();
        });
        $consumer->consume();
        $consumer->wait();
    }

    public function consumeFromBeginningAndEnd()
    {

        // This consumer will consume all existing records and all new records produced to 'fake-record'
        $consumerBeginning = (new ConsumerBuilder($this->brokers, 'beg', $this->schemaRegistryUrl))->build();
        $consumerBeginning->subscribe(FakeRecord::class, function ($record)
        {
            print '[consumerBeginning] id: ' . $record->getId() . PHP_EOL;
        });
        $consumerBeginning->consume();


        // This consumer will only consume records that are produced after it connects to kafka
        $consumerEnd = (new ConsumerBuilder($this->brokers, 'new', $this->schemaRegistryUrl))
          ->setOffsetReset('end')->build();
        $consumerEnd->subscribe(FakeRecord::class, function ($record)
        {
            print '[consumerEnd] id: ' . $record->getId() . PHP_EOL;
        });
        $consumerEnd->consume();

        $consumerEnd->wait();
        $consumerBeginning->wait();
    }
}

$ex = new ConsumerExample();
$ex->simpleConsume();