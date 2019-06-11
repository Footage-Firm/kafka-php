<?php

namespace App;

use App\Serializers\KafkaSerializerInterface;
use RdKafka\Conf as KafkaConfig;

// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

class Config extends KafkaConfig
{

    public const LOW_LATENCY_SOCKET_TIMEOUT = 50;

    public const LOW_LATENCY_QUEUE_BUFFERING_MAX = 1;

    protected $brokers;

    protected $serializer;

    protected $kafkaConfig;

    protected $shouldRegisterMissingSchemas = false;

    protected $shouldRegisterMissingSubjects = false;

    public function __construct(string $brokers, KafkaSerializerInterface $serializer)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();

        $this->serializer = $serializer;
        $this->setDefaultBrokers($brokers)->withLowLatencySettings();

    }

    // https://github.com/arnaud-lb/php-rdkafka#performance--low-latency-settings
    public function withLowLatencySettings(): Config
    {
        $this->setSocketTimeout(self::LOW_LATENCY_SOCKET_TIMEOUT);

        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, [SIGIO]);
            $this->set('internal.termination.signal', SIGIO);
            $this->setInternalTerminationSignal(SIGIO);
        } else {
            $this->setQueueBufferingMax(self::LOW_LATENCY_QUEUE_BUFFERING_MAX);
        }

        return $this;
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    private function setDefaultBrokers(string $brokers): Config
    {
        return $this->set('metadata.broker.list', $brokers);
    }

    public function setSocketTimeout(int $timeoutMs): Config
    {
        return $this->set('socket.timeout.ms', $timeoutMs);
    }

    public function setInternalTerminationSignal(int $signalValue): Config
    {
        return $this->set('internal.termination.signal', $signalValue);
    }

    public function setQueueBufferingMax(int $maxMs): Config
    {
        return $this->set('queue.buffering.max.ms', $maxMs);
    }

    public function setSslData(string $caLocation, string $certLocation, string $keyLocation): Config
    {
        return $this->set('security.protocol', 'ssl')
          ->set('ssl.ca.location', $caLocation)
          ->set('ssl.certificate.location', $certLocation)
          ->set('ssl.key.location', $keyLocation);
    }

    public function set($name, $value): Config
    {
        parent::set($name, $value);
        return $this;
    }
}