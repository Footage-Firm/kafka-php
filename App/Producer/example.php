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

    $topic = 'forPhpTesting';
    //    $schemaRegistryUri = 'schema-registry:8081';
    //    $brokers = 'broker';
    //    $topic = 'bbatest';
    //    $certLocation = '/opt/project/service.cert';
    //    $keyLocation = '/opt/project/service.key';
    //    $caLocation = '/opt/project/ca.pem';
    //    $client = new Client(['base_uri' => $schemaRegistryUri]);

    $schemaRegistryUri = 'https://sb-kafka-videoblocks-c6b6.aivencloud.com:26284';
    $schemaRegistryUser = 'avnadmin';
    $schemaRegistryPassword = 'ohsjbxv4adso0shh';
    $brokers = 'sb-kafka-videoblocks-c6b6.aivencloud.com:26281';
    $caLocation = 'ca.pem';
    $certLocation = 'service.cert';
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
    $producer = new Producer($config);

    //    for ($i = 0; $i <= 1000; $i++) {
    //        print "producing $i\n";
    //        $date = new DateTime();
    //        $d = $date->format('Y-m-d H:i:s');
    //        $meta1 = (new SharedMeta())->setUuid($d . '-' . $i);
    //        $userEventV1 = (new UserEvent())->setUserId($i)->setMeta($meta1)->setKey('key2');
    //
    //        $producer->produce($topic, $userEventV1);
    //    }

    $events = [];
    for ($i = 0; $i <= 1000; $i++) {
        $date = new DateTime();
        $d = $date->format('Y-m-d H:i:s');
        $meta = (new SharedMeta())->setUuid($d . '-' . $i);
        $events[] = (new UserEvent())->setUserId($i)->setMeta($meta);
    }
    $producer->produceMany($topic, $events);
}


produce();