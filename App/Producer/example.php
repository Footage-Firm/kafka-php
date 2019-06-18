<?php

namespace App;


use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V2\UserEvent;
use App\Producer\Producer;
use App\Producer\ProducerConfig;
use App\Serializers\AvroSerializer;
use DateTime;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use RdKafka\Message;

require_once __DIR__ . '/../../vendor/autoload.php';


function produce()
{

    //    $schemaRegistryUri = 'schema-registry:8081';
    $schemaRegistryUri = 'https://kafka-development-storyblocks-16cb.aivencloud.com:18367';
    $schemaRegistryUser = 'avnadmin';
    $schemaRegistryPassword = 'zlb2vwhmnp6opkvq';
    //    $brokers = 'broker';
    $brokers = 'kafka-development-storyblocks-16cb.aivencloud.com:18364';
    $topic = 'bbatest';

    //    $caLocation = '/opt/project/ca.pem';
    $caLocation = 'ca.pem';
    //    $certLocation = '/opt/project/service.cert';
    $certLocation = 'service.cert';
    //    $keyLocation = '/opt/project/service.key';
    $keyLocation = 'service.key';
    $client = new Client(['base_uri' => $schemaRegistryUri, 'auth' => [$schemaRegistryUser, $schemaRegistryPassword]]);
    $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
    $serializer = new AvroSerializer($registry, true, true);

    $config = new ProducerConfig($brokers, $serializer);
    $config->setSslData($caLocation, $certLocation, $keyLocation);
    $config->setDrMsgCb(function (\RdKafka\Producer $kafka, Message $message)
    {
        if ($message->err) {
            var_dump($message);
        } else {
            //worked
        }

    });
    $config->setPartition(1);
    $producer = new Producer($config);

    for ($i = 0; $i <= 1000; $i++) {
        print "producing $i\n";
        $date = new DateTime();
        $d = $date->format('Y-m-d H:i:s');
        $meta1 = (new SharedMeta())->setUuid($d . '-' . $i);
        $userEventV1 = (new UserEvent())->setUserId($i)->setMeta($meta1)->setKey('key2');

        $producer->fire($topic, $userEventV1);
    }
}


produce();