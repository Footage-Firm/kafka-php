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
    //    $caLocation = '/opt/project/ca.pem';
    $caLocation = '/../../ca.pem';
    //    $certLocation = '/opt/project/service.cert';
    $certLocation = '/../../service.cert';
    //    $keyLocation = '/opt/project/service.key';
    $keyLocation = '/../../service.key';
    $client = new Client(['base_uri' => $schemaRegistryUri, 'auth' => [$schemaRegistryUser, $schemaRegistryPassword]]);
    $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
    $serializer = new AvroSerializer($registry);//, true, true);

    $config = new ConsumerConfig($brokers, 'ng', $serializer);
    $config->setSslData($caLocation, $certLocation, $keyLocation);
    $config->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null)
    {
        switch ($err) {
            case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Assign: ";
                var_dump($partitions);
                $kafka->assign($partitions);
                break;

            case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                echo "~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~Revoke: ";
                var_dump($partitions);
                $kafka->assign(null);
                break;

            default:
                throw new \Exception($err);
        }
    });
    $topics = ['bbatest'];

    echo 'Consuming topics: ' . implode(',', $topics) . PHP_EOL;

    $consumer = new Consumer($config);

    $consumer->onSuccess(function (UserEvent $userEvent)
    {
        print $userEvent->getUserId() . ", " . PHP_EOL;
    });
    $consumer->onError(function ()
    {
        echo 'An error has occurred';
    });

    $consumer->consume($topics, new UserEvent());
}

consume();