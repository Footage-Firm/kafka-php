<?php

namespace App;


use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V2\UserEvent;
use App\Producer\Producer;
use App\Producer\ProducerConfig;
use App\Serializers\AvroSerializer;
use DateTime;
use Faker\Factory;
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
    $topic = 'user-event';

    $caLocation = '/opt/project/ca.pem';
    $certLocation = '/opt/project/service.cert';
    $keyLocation = '/opt/project/service.key';

    $serializer = AvroSerializer::createWithDefaultRegistry($schemaRegistryUri, $schemaRegistryUser,
      $schemaRegistryPassword)
      ->shouldRegisterMissingSubjects(true)
      ->shouldRegisterMissingSchemas(true);


    $config = new ProducerConfig($brokers, $serializer);
    $config->setSslData($caLocation, $certLocation, $keyLocation);
    $config->setDrMsgCb(function (\RdKafka\Producer $kafka, Message $message)
    {
        if ($message->err) {
            var_dump($message);
        } else {
            echo 'Looks like it worked' . PHP_EOL;
        }

    });

    $producer = new Producer($config, $serializer);

    $faker = Factory::create();

    for ($i = 1; $i <= 10; $i++) {
        $date = new DateTime();
        $d = $date->format('Y-m-d H:i:s');
        echo "Producing topic: $topic" . PHP_EOL;
        $meta1 = (new SharedMeta())->setUuid($d . '-' . $i);
        $userEventV1 = (new UserEvent())->setUserId($faker->randomDigit)->setMeta($meta1);

        $producer->fire($topic, $userEventV1);
    }
}


produce();