<?php

namespace App\Serializers;

use App\Events\BaseRecord;
use App\Serializers\Traits\SnakeCaseFormatterTrait;
use AvroSchema;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;
use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
use GuzzleHttp\Client;
use RdKafka\Message;

class AvroSerializer implements KafkaSerializerInterface
{

    use SnakeCaseFormatterTrait;

    private $serializer;

    private $shouldRegisterMissingSchemas = false;

    private $shouldRegisterMissingSubjects = false;

    public function __construct(Registry $registry)
    {
        $this->serializer = $this->createSerializer($registry);
    }

    public static function createWithDefaultRegistry(
      string $schemaRegistryUri,
      string $username,
      string $password
    ): AvroSerializer {
        $client = new Client(['base_uri' => $schemaRegistryUri, 'auth' => [$username, $password]]);
        $registry = new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());

        return new self($registry);
    }

    public function serialize(BaseRecord $record): string
    {
        $schema = AvroSchema::parse($record->schema());
        $data = $record->data();
        $name = str_replace('_', '-', $this->convertToSnakeCase($record->name()));

        return $this->serializer->encodeRecord($name . '-value', $schema, $data);
    }

    public function deserialize(Message $message, BaseRecord $record): BaseRecord
    {
        try {
            $decoded = $this->serializer->decodeMessage($message->payload);
            $record->decode($decoded);
            return $record;
        } catch (AvroDecodingException $e) {
            $prev = $e->getPrevious();

            // parse the reader/writer types
            $matches = [];
            preg_match(
              '/Writer\'s schema .*?"name":"(\w+)".*?Reader\'s schema .*?"name":"(\w+)"/',
              $prev->getMessage(),
              $matches
            );
            [$_, $writerType, $readerType] = $matches;

            echo ">>> Skipping message. writerType: $writerType, readerType: $readerType" . PHP_EOL;
        }
    }

    private function createSerializer(Registry $registry): RecordSerializer
    {
        return new RecordSerializer(
          $registry,
          [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => $this->shouldRegisterMissingSchemas,
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => $this->shouldRegisterMissingSubjects,
          ]
        );
    }

    public function shouldRegisterMissingSchemas(bool $shouldRegisterMissingSchemas): AvroSerializer
    {
        $this->shouldRegisterMissingSchemas = $shouldRegisterMissingSchemas;
        return $this;
    }

    public function shouldRegisterMissingSubjects(bool $shouldRegisterMissingSubjects): AvroSerializer
    {
        $this->shouldRegisterMissingSubjects = $shouldRegisterMissingSubjects;
        return $this;
    }

}