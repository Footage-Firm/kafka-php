<?php

namespace App\Consumer;

use RdKafka\TopicConf;

class ConsumerLow
{

    public function consume()
    {
        $topicConf = new TopicConf();
        $topicConf->set('auto.commit.enable', 'false');
        $consumer = new \RdKafka\Consumer();
        $consumer->addBrokers('broker');
        $topic = $consumer->newTopic('test', $topicConf);
        $topic->consumeStart(0, 9);

        $msg = $topic->consume(0, 10000);
        var_dump($msg);
    }
}