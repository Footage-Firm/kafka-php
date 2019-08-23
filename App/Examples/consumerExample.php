<?php

namespace App\Example;

use App\Consumer\ConsumerBuilder;
use Tests\Fakes\FakeRecord;

require_once __DIR__ . '/../../vendor/autoload.php';

function c()
{
    $schemaRegistryUrl = "http://0.0.0.0:8081";
    $brokers = ["0.0.0.0:29092"];
    $b = new ConsumerBuilder($brokers, 'abcdef', $schemaRegistryUrl);
    $c = $b->build();
    $c->consume(['xxxxx'], new FakeRecord());
}

function consunerFromAiven()
{
    $brokers = ['storyblocks-prod-storyblocks-16cb.aivencloud.com:18364'];
    $schemaRegistryUrl = 'https://avnadmin:jw49xwkamnu321kl@storyblocks-prod-storyblocks-16cb.aivencloud.com:18367';
    $builder = new ConsumerBuilder($brokers, 'kafka-php-group2', $schemaRegistryUrl);
    $base = '/Users/brendanalexander/Projects/kafka/ssl/';
    $builder->setSslData($base . 'ca.pem', $base . 'service.cert', $base . 'service.key');
    $consumer = $builder->build();
    $consumer->subscribe(new FakeRecord(), function ($record)
    {
        var_dump($record);
    });
    $consumer->consume(['kafka-php']);
}

consunerFromAiven();