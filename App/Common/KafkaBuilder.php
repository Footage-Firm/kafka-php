<?php

namespace App\Common;

use RdKafka\Conf;

abstract class KafkaBuilder
{

    protected $config;

    public function __construct(Conf $config = null)
    {
        $this->config = $config ?? new Conf();
    }

    public function onKafkaError(callable $callback): KafkaBuilder
    {
        $this->config->setErrorCb($callback);
        return $this;
        $this->config->cb
    }
}