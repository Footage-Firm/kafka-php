<?php

namespace App\Serializers;

use App\Events\BaseRecord;
use App\Traits\RecordFormatting;
use AvroSchema;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;

class AvroSerializer implements KafkaSerializerInterface
{

    use RecordFormatting;

    private $serializer;

    public function __construct(
      Registry $registry,
      bool $shouldRegisterMissingSchemas = false,
      bool $shouldRegisterMissingSubjects = false
    ) {
        $this->serializer = $this->createSerializer(
          $registry,
          $shouldRegisterMissingSchemas,
          $shouldRegisterMissingSubjects
        );
    }

    public function serialize(BaseRecord $record): string
    {
        $schema = AvroSchema::parse($record->schema());
        $data = $record->data();
        $name = str_replace('_', '-', $this->convertToSnakeCase($record->name()));

        return $this->serializer->encodeRecord($name . '-value', $schema, $data);
    }

    public function deserialize(string $payload, BaseRecord $record): BaseRecord
    {
        try {
            $decoded = $this->serializer->decodeMessage($payload);
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

    private function createSerializer(
      Registry $registry,
      bool $shouldRegisterMissingSchemas,
      bool $shouldRegisterMissingSubjects
    ): RecordSerializer {
        return new RecordSerializer(
          $registry,
          [
            RecordSerializer::OPTION_REGISTER_MISSING_SCHEMAS => $shouldRegisterMissingSchemas,
            RecordSerializer::OPTION_REGISTER_MISSING_SUBJECTS => $shouldRegisterMissingSubjects,
          ]
        );
    }


}