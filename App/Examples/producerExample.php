<?php

namespace App\Example;

use App\Producer\ProducerBuilder;
use Tests\Fakes\FakeFactory;
use Throwable;

require_once __DIR__ . '/../../vendor/autoload.php';

class Example
{

    private $schemaRegistryUrl = 'http://0.0.0.0:8081';

    private $brokers = ['0.0.0.0:29092'];

    function simpleProducer(): void
    {
        $fakeRecord = FakeFactory::fakeRecord();

        $producer = (new ProducerBuilder($this->brokers, $this->schemaRegistryUrl))->build();

        try {
            $producer->produce($fakeRecord);
        } catch (Throwable $t) {
            print 'There was an error! ' . $t->getMessage();
        }
    }

    function producerWithCallbacks(): void
    {
        $fakeRecord = FakeFactory::fakeRecord();

        $producer = (new ProducerBuilder($this->brokers, $this->schemaRegistryUrl))
          ->setDeliveryReportCallback(function ($kafka, $message)
          {
              if ($message->offset) {
                  print 'Offset: ' . $message->offset;
              } else {
                  print 'There was a problem';
              }
          })
          ->setKafkaErrorCallback(function (\Rdkafka\Producer $kafka, int $err, string $reason)
          {
              // This callback will be triggered when lib-rdkafka runs into a problem.
              printf('Kafka ran into error %s because %s', $err, $reason);
          })
          ->build();
        
        try {
            $producer->produce($fakeRecord);
        } catch (Throwable $t) {
            print 'There was an error! ' . $t->getMessage();
        }
    }

}

$ex = new Example();
$ex->simpleProducer();