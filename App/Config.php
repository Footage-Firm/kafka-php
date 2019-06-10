<?php

namespace App;

use RdKafka\Conf as KafkaConfig;

// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

class Config extends KafkaConfig
{

    protected $schemaRegistryUri;

    protected $brokers;

    protected $kafkaConfig;

    protected $shouldRegisterMissingSchemas = false;

    protected $shouldRegisterMissingSubjects = false;

    public function __construct(string $schemaRegistryUri, string $brokers)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();
        $this->schemaRegistryUri = $schemaRegistryUri;
        $this->setDefaultBrokers($brokers);

        // https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings
        $this->set('socket.timeout.ms', 50);
        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, [SIGIO]);
            $this->set('internal.termination.signal', SIGIO);
        } else {
            $this->set('queue.buffering.max.ms', 1);
        }

    }

    public function getSchemaRegistryUri(): string
    {
        return $this->schemaRegistryUri;
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    private function setDefaultBrokers(string $brokers): void
    {
        $this->set('metadata.broker.list', $brokers);
    }

    public function shouldRegisterMissingSchemas(): bool
    {
        return $this->shouldRegisterMissingSchemas;
    }

    public function setShouldRegisterMissingSchemas(bool $registerMissingSchemas): Config
    {
        $this->shouldRegisterMissingSchemas = $registerMissingSchemas;
        return $this;
    }

    public function shouldRegisterMissingSubjects(): bool
    {
        return $this->shouldRegisterMissingSubjects;
    }

    public function setShouldRegisterMissingSubjects(bool $registerMissingSubjects): Config
    {
        $this->shouldRegisterMissingSubjects = $registerMissingSubjects;
        return $this;
    }

    public function set($name, $value): Config
    {
        parent::set($name, $value);
        return $this;
    }
}