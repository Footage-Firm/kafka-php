<?php

namespace App\Example;

use App\Producer\ProducerBuilder;
use Exception;
use RdKafka\Message;
use Tests\Fakes\FakeRecordFactory;

require_once __DIR__ . '/../../vendor/autoload.php';

function produceLocally()
{
    $schemaRegistryUrl = "http://0.0.0.0:8081";
    $brokers = ["0.0.0.0:29092"];
    $fakeRecord = FakeRecordFactory::fakeRecord();
    $fakeRecord->setName('the name');
    $builder = new ProducerBuilder($brokers, $schemaRegistryUrl);
    $producer = $builder->build();
    try {
        $producer->produce($fakeRecord, 'xxxxx');
    } catch (Exception $exception) {
        print($exception->getMessage());
    }
}

function produceToAiven()
{
    $brokers = ['storyblocks-prod-storyblocks-16cb.aivencloud.com:18364'];
    $schemaRegistryUrl = 'https://avnadmin:jw49xwkamnu321kl@storyblocks-prod-storyblocks-16cb.aivencloud.com:18367';
    $builder = new ProducerBuilder($brokers, $schemaRegistryUrl);
    $base = '/Users/brendanalexander/Projects/kafka/ssl/';
    $builder->setSslData($base . 'ca.pem', $base . 'service.cert', $base . 'service.key');
    $builder->setDeliveryReportCallback(function ($kafka, Message $message)
    {
        print $message->offset . "\n";
    });
    $fakeRecord = FakeRecordFactory::fakeRecord();
    $producer = $builder->build();
    try {
        for ($i = 0; $i < 20; $i++) {
            $producer->produce($fakeRecord, 'kafka-php');
        }
    } catch (Exception $exception) {
        print($exception->getMessage());
    }
}

produceLocally();
//produceToAiven();