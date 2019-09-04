<?php


namespace Test\Consumer\Integration;

use App\Consumer\ConsumerBuilder;
use App\Events\BaseRecord;
use App\Producer\Producer;
use App\Producer\ProducerBuilder;
use PHPUnit\Framework\TestCase;
use Tests\WithFaker;

class TestConsumerSchemaEvolution extends TestCase
{

    use WithFaker;

    private $schemaRegistryUrl = 'http://0.0.0.0:8081';

    private $brokers = ['0.0.0.0:29092'];

    private $groupId;

    private $fileName = 'schemaEvolution.txt';

    public function setUp(): void
    {
        parent::setUp();
        $this->initFaker();
        $this->groupId = $this->faker->word;
    }

    public function tearDown(): void
    {
        parent::tearDown();
        if (file_exists($this->fileName)) {
            unlink(realpath($this->fileName));
        }
    }

    public function testConsumerCanReadRecordWithUpdatedSchema(): void
    {

        //
        // Produce 5 records with EvolvingRecord schema, then update the schema by adding 'NewField' and produce 5 more records
        //
        $topic = $this->faker->word;
        print "Using topic $topic and group id $this->groupId\n";
        $producer = (new ProducerBuilder($this->brokers, $this->schemaRegistryUrl))
          ->shouldSendToFailureTopic(false)
          ->build();

        print "Producing records with initial schema...\n";
        $originalRecords = $this->produceInitialRecords($producer, $topic);
        print "Producing records with updated schema...\n";
        $updatedRecords = $this->produceUpdatedRecords($producer, $topic);

        //
        // Create a consumer that listens to the topic with EvolvingRecord. This will first read the events produced
        // with the old schema, then the new schema, and no errors should be thrown.
        //
        $consumer = (new ConsumerBuilder($this->brokers, $this->groupId, $this->schemaRegistryUrl))->build();
        $consumer->setConsumerLifetime(45);

        $fileName = 'schemaEvolution.txt';
        $consumer->subscribe(EvolvingRecord::class, function ($record) use ($fileName)
        {
            file_put_contents($fileName, json_encode($record) . ';', FILE_APPEND | LOCK_EX);
        });
        $consumer->consume($topic);

        $contents = file_get_contents($fileName);
        $lines = array_filter(explode(';', $contents));
        $this->assertCount(10, $lines);

        $consumedOriginalRecords = [];
        $consumedUpdatedRecords = [];

        foreach ($lines as $line) {
            $decoded = json_decode($line);
            if (property_exists($decoded, 'newField')) {
                $consumedUpdatedRecords[] = $decoded;
                $updated = array_filter($updatedRecords, function ($updated) use ($decoded)
                {
                    return $updated->id === $decoded->id;
                });
                $this->assertNotNull($updated);
            } else {
                $consumedOriginalRecords[] = $decoded;
                $original = array_filter($originalRecords, function ($original) use ($decoded)
                {
                    return $original->id === $decoded->id;
                });
                $this->assertNotNull($original);
            }
        }

        $this->assertCount(5, $consumedOriginalRecords);
        $this->assertCount(5, $consumedUpdatedRecords);
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