<?php


namespace Tests\Consumer\Integration;

use KafkaPhp\Consumer\ConsumerBuilder;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Producer\ProducerBuilder;
use EventsPhp\BaseRecord;
use Tests\BaseTestCase;

class ConsumerSchemaEvolutionTest extends BaseTestCase
{

    private $groupId;

    public function setUp(): void
    {
        parent::setUp();
        $this->groupId = $this->faker()->word;
    }

    public function testConsumerCanReadRecordWithUpdatedSchema(): void
    {

        //
        // Produce 5 records with EvolvingRecord schema, then update the schema by adding 'NewField' and produce 5 more records
        //
        $topic = $this->faker()->word;
        print "Using topic $topic and group id $this->groupId\n";
        $producer = (new ProducerBuilder($this->brokerHosts, $this->schemaRegistryUrl))
          ->shouldSendToFailureTopic(false)
          ->build();

        $originalRecords = $this->produceInitialRecords($producer, $topic);
        $updatedRecords = $this->produceUpdatedRecords($producer, $topic);

        //
        // Create a consumer that listens to the topic with EvolvingRecord. This will first read the events produced
        // with the old schema, then the new schema, and no errors should be thrown.
        //
        $consumer = (new ConsumerBuilder($this->brokerHosts, $this->groupId, $this->schemaRegistryUrl))
          ->setNumRetries(0)
          ->build();

        $records = [];
        $consumer->subscribe(EvolvingRecord::class, function (EvolvingRecord $record) use (&$records, &$consumer)
        {
            array_push($records, $record);

            if (count($records) >= 10) {
                $consumer->disconnect();
            }
        });

        $consumer->consume($topic);
        $consumer->wait();

        $this->assertCount(10, $records);

    }


    private function produceInitialRecords(Producer $producer, string $topic)
    {
        return array_map(function ($i) use ($producer, $topic)
        {
            $fakeRecord = new EvolvingRecord();
            $fakeRecord->id = $i;
            $producer->produce($fakeRecord, $topic);
            return $fakeRecord;
        }, range(0, 4)
        );
    }

    private function produceUpdatedRecords(Producer $producer, string $topic)
    {
        return array_map(function ($i) use ($producer, $topic)
        {
            $fakeRecord = new EvolvingRecord();
            $fakeRecord->schema = $this->getUpdatedSchema();
            $fakeRecord->id = $i;
            $fakeRecord->newField = 'NEW FIELD';
            $producer->produce($fakeRecord, $topic);
            return $fakeRecord;
        }, range(5, 9)
        );
    }

    private function getUpdatedSchema()
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "EvolvingRecord",
    "namespace": "testing",
    "fields": [
      { "name": "id", "type": "int" },
      { "name": "newField", "type": ["string","null"], "default": "ABC" } 
    ]
  }
SCHEMA;
    }
}

class EvolvingRecord extends BaseRecord
{

    public $schema = <<<SCHEMA
{
    "type": "record",
    "name": "EvolvingRecord",
    "namespace": "testing",
    "fields": [
      { "name": "id", "type": "int" }
    ]
  }
SCHEMA;

    public $id;

    public function schema(): string
    {
        return $this->schema;
    }

    public function jsonSerialize()
    {
        $serialized = ['id' => $this->encode($this->id)];
        if (property_exists($this, 'newField')) {
            $serialized['newField'] = $this->encode($this->newField);
        }

        return $serialized;
    }
}