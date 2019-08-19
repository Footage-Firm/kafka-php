<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatting;
use AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use GuzzleHttp\Promise\PromiseInterface;
use RdKafka\Message;
use function FlixTech\AvroSerializer\Protocol\decode;
use function Widmogrod\Functional\valueOf;

class RecordProcessor
{

    use RecordFormatting;

    /** @var MessageHandler[] */
    private $handlers = [];

    private $registry;

    private $serializer;

    public function __construct(Registry $registry, KafkaSerializerInterface $serializer)
    {
        $this->registry = $registry;
        $this->serializer = $serializer;
    }

    public function subscribe(BaseRecord $record, callable $success, ?callable $failure): void
    {
        $this->handlers[$this->className($record)] = new MessageHandler(
          $record,
          $success,
          $this->getSchemaId($record),
          $failure
        );
    }

    public function process(Message $message)
    {
        $handler = $this->getHandlerForMessage($message);
        $record = $this->serializer->deserialize($message->payload, $handler->getRecord());
        $handler->success();
        // get correct handler, try to process, retry if errors
    }

    private function getSchemaId(BaseRecord $record): int
    {
        // this is
        $subject = $this->convertToSnakeCase($record->name(), '-') . '-value';
        $schema = AvroSchema::parse($record->schema());
        $response = $this->registry->schemaId($subject, $schema);
        return $this->extractValueFromRegistryResponse($response);

    }

    private function extractValueFromRegistryResponse($response): int
    {
        if ($response instanceof PromiseInterface) {
            $response = $response->wait();
        }

        if ($response instanceof \Exception) {
            throw $response;
        }

        return $response;
    }

    private function getHandlerBySchemaId(int $schemaId): ?MessageHandler
    {
        foreach ($this->handlers as $i => $handler) {
            if ($handler->getSchemaId() === $schemaId) {
                return $this->handlers[$i];
            }
        }
        // todo -- handle this case
    }

    public function getHandlers(): array
    {
        return $this->handlers;
    }

    private function getHandlerForMessage(Message $message)
    {
        $schemaId = $this->getSchemaIdFromMessage($message);
        return $this->getHandlerBySchemaId($schemaId);
    }

    private function getSchemaIdFromMessage(Message $message)
    {
        $decoded = valueOf(decode($message->payload));
        if (is_array($decoded) && array_key_exists('schemaId', $decoded)) {
            return $decoded['schemaId'];
        }
        // todo -- handle exception here
        throw new \RuntimeException();
    }

}