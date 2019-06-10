<?php

namespace App;


use App\Events\Poc\Common\SharedMeta;
use App\Events\Poc\User\V1\UserEvent;
use App\Producer\Producer;
use App\Producer\ProducerConfig;
use DateTime;
use Faker\Factory;
use RdKafka;
use RdKafka\Message;

require_once __DIR__ . '/../vendor/autoload.php';


function produce()
{
    $topic = 'newtopic';
    $config = new ProducerConfig('schema-registry:8081', 'broker');

    $config->setShouldRegisterMissingSchemas(true);
    $config->setShouldRegisterMissingSubjects(true);
    $config->setDrMsgCb(function (RdKafka $kafka, Message $message)
    {
        $message->err;
        var_dump($message);
    });
    $producer = new Producer($config);


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