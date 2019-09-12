# php-kafka

An opinionated Kafka producer & consumer library for PHP.

## Dependencies
 
 - PHP 7.x 
 - [librdkafka](https://github.com/edenhill/librdkafka) ([Installation instructions](https://arnaud.le-blanc.net/php-rdkafka/phpdoc/rdkafka.installation.html))
 
 ## About
 
 > This documentation refers to our Producers & Consumers, not necessarily Kafka Producers and Consumers in general.
 
 This project provides a convenient way to Producer and Consume Avro-encoded records for Kafka. It uses Confluent's
 schema registry and (php-rdkafka)[https://github.com/arnaud-lb/php-rdkafka]. 
 

### Core Kafka Concepts

Kafka stores records in a fast, highly-available cluster. The records are organized into topics, much like
how the data of a relational database is organized using tables. The major difference being that a Kafka topic
is an append-only commit log ([a good article about logs](https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying))
instead of a mutable data store. Records are sent to Kafka using a `Producer`, and records are read from Kafka using a `Consumer`.

Each topic can have one or more partitions. Partitioning a topics allow for the concurrent processing of records in that
topic. Each topic also has a "replication factor" which is the number of copies of the topic that exist.

Each record in Kafka is assigned an offset. Each record in a partition of a topic gets a unique identifier. In this image
the vertical numbers are the record offsets:

![Anatomy of a topic](docs/AnatomyOfATopic.png)

This project mandates the use of [Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) to
store/retrieve/validate the schemas of the data being sent to Kafka.

### Producing

Create a producer using the `ProducerBuilder` class. There are many configuration options available, but the
defaults are a good starting point. An important method on the builder class is `setSslData`. Use this to
set the location of the certificate and keys if necessary.

```php
$builder = new ProducerBuilder(['brokers.go.here:123'], 'http://schemaRegitry.url');
$producer = $builder->setSslData('ca.pem', 'service.cert', 'service.key')->build();
```

Producers emit messages to kafka. These messages are an avro-encoded record of whatever data you want to send.
The Producers only understand data that is in the form of a class that extends `BaseRecord`.

```php
<?php
use EventsPhp\BaseRecord;
use App\Producer\ProducerBuilder;

class Duck extends BaseRecord {
    private $id;

    public function setId($id): void
    {
        $this->id = $id;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "{$this->name()}",
    "namespace": "pond",
    "fields": [{ "name": "id", "type": "int" }]
}
SCHEMA;
    }

    public function jsonSerialize()
    {
        return [
          'id' => $this->encode($this->id),
        ];
    } 
}

$producer = (new ProducerBuilder(['brokers.go.here:123'], 'http://schemaRegitry.url'))->build();
$producer->produce(new Duck());
```

The `produce` function doesn't return anything. To get information about the produced record, add a delivery report callback.

```php
    $builder = (new ProducerBuilder($brokers, $schemaRegistryUrl))
        ->setDeliveryReportCallback(function (RdKafka\RdKafka $kafka, Message $message)
            {
                print "The offset of the produced message is " . $message->offset . PHP_EOL;
            });
```


#### Topics

As mentioned above, Kafka organized records into topics similar to how a relational database stored data in tables.
By default, the topic name is the kebab-case class name of the record being produced. This can be changed by sending
an optional second parameter to the `produce` method.

```typescript
// This will produce to the topic 'duck'
$producer->produce(new Duck());

// This will produce to the topic 'pond-population'
$producer->produce(new Duck(), 'pond-population');
```

#### Failures

If there is an error when trying to produce a record, the data is captured in a `Failure` record and written to a
special failure topic: `fail-kebab-case-record-name`. Writing to the failure topic can be disabled, and the number of
retries is also configurable.

#### Guarantees

The default settings of the Producer give an exactly once delivery guarantee (this is done by setting `enable.idempotence`
to `true`). Kafka also guarantees that messages sent by a producer to a particular topic partition will be stored in the same
order they were sent. Note that this guarantee is on a per partition basis, not per topic.

Kafka also guarantees that a topic will tolerate N-1 server failures if the topic has a replication factor of N.

[Read more about Kafka Guarantees here](https://kafka.apache.org/documentation/#intro_guarantees).

#### Examples

See `/App/examples/producerExample.php` for a working example with comments.
