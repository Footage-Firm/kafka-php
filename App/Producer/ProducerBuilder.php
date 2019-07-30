<?php

namespace App\Producer;

use App\Common\KafkaBuilder;
use App\Serializers\AvroSerializer;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use Psr\Log\LoggerInterface;
use RdKafka\Conf;
use RdKafka\Producer as KafkaProducer;

class ProducerBuilder extends KafkaBuilder
{


    private $serializer;

    private $logger;

    public function __construct(array $brokers, string $schemaRegistryUri, LoggerInterface $logger, Conf $config = null)
    {
        parent::__construct($config);


        $this->config->set('metadata.broker.list', implode(',', $brokers));
        $client = new Client([
          'base_uri' => $schemaRegistryUri,
            //todo -- parse the url for auth
            //          'auth' => [$schemaRegistryUser, $schemaRegistryPassword],
        ]);
        $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
        $serializer = new AvroSerializer($registry, true, true);
        $this->config->set('acks', -1); //todo -- comment/constant so this is clear
        $this->config->set('enable.idempotence', true);
        // When set to true, the producer will ensure that messages are successfully produced exactly once and in the original produce order. The following configuration properties are adjusted automatically (if not modified by the user) when idempotence is enabled: max.in.flight.requests.per.connection=5 (must be less than or equal to 5), retries=INT32_MAX (must be greater than 0), acks=all, queuing.strategy=fifo. Producer instantation will fail if user-supplied configuration is incompatible. 
    }

    public function build(): Producer
    {
        $kafkaProducer = new KafkaProducer($this->config);
        return new Producer($kafkaProducer, $this->serializer, $this->logger);
    }

    private function createKafkaProducer(): KafkaProducer
    {
        $kafkaProducer = new KafkaProducer($this->config);

        //        $logLevel = $this->config->getLogLevel();
        //        if ($logLevel) {
        //            $producer->setLogLevel(LOG_DEBUG);
        //        }

        return $kafkaProducer;
    }

    private function setSslData(string $caPath, string $certPath, string $keyPath): void
    {
        $this->config->set('security.protocol', 'ssl');
        $this->config->set('ssl.ca.Path', $caPath);
        $this->config->set('ssl.certificate.Path', $certPath);
        $this->config->set('ssl.key.Path', $keyPath);
    }

    // Signature of the callback function is function (RdKafka\RdKafka $kafka, RdKafka\Message $message);
    public function setDeliveryReportCallback(callable $callback): void
    {
        $this->config->setDrMsgCb($callback);
    }


}