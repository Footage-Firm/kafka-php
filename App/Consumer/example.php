<?php


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\User\V2\UserEvent;
use App\Serializers\AvroSerializer;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;

require_once __DIR__ . '/../../vendor/autoload.php';

function consume()
{
    //    $schemaRegistryUri = 'schema-registry:8081';
    $schemaRegistryUri = 'https://kafka-development-storyblocks-16cb.aivencloud.com:18367';
    $schemaRegistryUser = 'avnadmin';
    $schemaRegistryPassword = 'zlb2vwhmnp6opkvq';
    //    $brokers = 'broker';
    $brokers = 'kafka-development-storyblocks-16cb.aivencloud.com:18364';
    //    $topic = 'user-event';
    $caLocation = '/opt/project/ca.pem';
    $certLocation = '/opt/project/service.cert';
    $keyLocation = '/opt/project/service.key';

    $client = new Client(['base_uri' => $schemaRegistryUri, 'auth' => [$schemaRegistryUser, $schemaRegistryPassword]]);
    $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
    $serializer = new AvroSerializer($registry);//, true, true);

    $config = new ConsumerConfig($brokers, 'brendanGroup', $serializer);
    $config->setSslData($caLocation, $certLocation, $keyLocation);

    $topics = ['bbatest'];


    echo 'Consuming topics: ' . implode(',', $topics) . PHP_EOL;

    $consumer = new Consumer($config);

    $consumer->onSuccess(function (UserEvent $userEvent)
    {
        print "OK success";
        $x = json_encode($userEvent, JSON_PRETTY_PRINT) . PHP_EOL;
        var_dump($x);
        return $x;
    });
    $consumer->onError(function ()
    {
        echo 'An error has occurred';
    });

    $consumer->consume($topics, new UserEvent());
}

consume();