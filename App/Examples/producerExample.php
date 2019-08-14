<?php

namespace App\Example;

use App\Producer\ProducerBuilder;
use Tests\Fakes\FakeRecordFactory;

require_once __DIR__ . '/../../vendor/autoload.php';

function produceLocally()
{
    $topic = 'abc123';
    $schemaRegistryUrl = "http://0.0.0.0:8081";
    $brokers = ["0.0.0.0:29092"];
    $fakeRecord = FakeRecordFactory::fakeRecord();

    $builder = new ProducerBuilder($brokers, $schemaRegistryUrl);
    $producer = $builder->build();
    $producer->produce($fakeRecord, $topic);


}
