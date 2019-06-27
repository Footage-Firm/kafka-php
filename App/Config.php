<?php

namespace App;

use App\Logger\Logger;
use App\Serializers\KafkaSerializerInterface;
use Psr\Log\LoggerInterface;
use RdKafka\Conf as KafkaConfig;

// https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md

class Config extends KafkaConfig
{

    public const LOW_LATENCY_SOCKET_TIMEOUT = 50;

    public const LOW_LATENCY_QUEUE_BUFFERING_MAX = 1;

    protected $brokers;

    protected $serializer;

    protected $logger;

    protected $kafkaConfig;

    protected $shouldRegisterMissingSchemas = false;

    protected $shouldRegisterMissingSubjects = false;

    public function __construct(string $brokers, KafkaSerializerInterface $serializer, LoggerInterface $logger = null)
    {
        //Ignore IDE squiggly, there is a constructor its just not int he stub extension
        parent::__construct();

        $this->serializer = $serializer;
        $this->setDefaultBrokers($brokers);
        $this->logger = $logger ?? new Logger();
    }

    public function getBrokers(): string
    {
        return $this->brokers;
    }

    public function getSerializer(): KafkaSerializerInterface
    {
        return $this->serializer;
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