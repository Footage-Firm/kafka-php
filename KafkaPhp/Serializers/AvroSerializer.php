<?php

namespace KafkaPhp\Serializers;

use GuzzleHttp\Exception\ConnectException;
use KafkaPhp\Serializers\Errors\ProducerTimeoutError;
use KafkaPhp\Traits\RecordFormatter;
use AvroSchema;
use EventsPhp\BaseRecord;
use FlixTech\AvroSerializer\Objects\Exceptions\AvroDecodingException;
use FlixTech\AvroSerializer\Objects\RecordSerializer;
use FlixTech\SchemaRegistryApi\Registry;
use GuzzleHttp\Promise\PromiseInterface;
use function FlixTech\AvroSerializer\Protocol\decode;
use function Widmogrod\Functional\valueOf;

class AvroSerializer implements KafkaSerializerInterface
{

    use RecordFormatter;

    private $serializer;

    private $registry;

    public function __construct(
      Registry $registry,
      bool $shouldRegisterMissingSchemas = false,
      bool $shouldRegisterMissingSubjects = false
    ) {
        $this->registry = $registry;
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
        $name = $this->kebabCase($record->name());

        try {
            return $this->serializer->encodeRecord($name . '-value', $schema, $data);
        } catch (\RuntimeException $e) {
            throw new ProducerTimeoutError('Error encoding record.', null, $e);
        }
    }

    public function deserialize(string $payload): array
    {
        $schemaName = $this->nameFromEncodedPayload($payload);
        try {
            $decoded = $this->serializer->decodeMessage($payload);
            return [$schemaName => $decoded];
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

    private function nameFromEncodedPayload(string $payload): string
    {
        $schemaId = $this->getSchemaId($payload);
        /** @var \AvroRecordSchema $s */
        $avroRecordSchema = $this->extractValueFromRegistryResponse($this->registry->schemaForId($schemaId));
        $name = explode('.', $avroRecordSchema->qualified_name());
        return array_pop($name);
    }

    private function getSchemaId(string $payload)
    {
        $decoded = valueOf(decode($payload));
        return is_array($decoded) && array_key_exists('schemaId', $decoded)
          ? $decoded['schemaId']
          : null;

    }

    private function extractValueFromRegistryResponse($response)
    {
        if ($response instanceof PromiseInterface) {
            $response = $response->wait();
        }

        if ($response instanceof \Exception) {
            throw $response;
        }

        return $response;
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