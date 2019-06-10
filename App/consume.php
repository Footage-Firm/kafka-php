<?php

namespace App;


use App\Consumer\Consumer;
use App\Consumer\ConsumerConfig;
use App\Events\Poc\User\V1\UserEvent;

require_once __DIR__ . '/../vendor/autoload.php';


function consume()
{
    $config = new ConsumerConfig('groupId', 'schema-registry:8081', 'broker');
    $topics = ['newtopic'];

    echo 'Consuming topics: ' . implode(',', $topics) . PHP_EOL;

    $consumer = new Consumer($config, $topics);

    $consumer->onSuccess(function (UserEvent $userEvent)
    {
        return json_encode($userEvent, JSON_PRETTY_PRINT) . PHP_EOL;
    });
    $consumer->onError(function ()
    {
        echo 'An error has occurred';
    });

    //    $res = $consumer->consumeUntilEnd(new UserEvent());
    $consumer->consume(new UserEvent());
    var_dump($res);
}

consume();