<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatting;
use AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use GuzzleHttp\Promise\PromiseInterface;
use RdKafka\Message;
use Throwable;
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

    public function subscribe(
      BaseRecord $record,
      callable $success,
      callable $failure = null
    ): void {
        $this->handlers[$this->className($record)] = new MessageHandler(
          $record,
          $success,
          $this->schemaIdFromRecord($record),
          $failure
        );
    }

    public function process(Message $message)
    {
        $handler = $this->getHandlerForMessage($message);

        if ($handler) {
            try {
                $decoded = $this->serializer->deserialize($message->payload, $handler->getRecord());
                return $handler->success($decoded);
            } catch (Throwable $t) {

            }
        }
    }

    protected function retry($handler)
    {

    }

    private function schemaIdFromRecord(BaseRecord $record): int
    {
        $subject = $this->formatAsSubject($record->name());
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
        return null;
    }

    private function getHandlerForMessage(Message $message)
    {
        $schemaId = $this->getSchemaIdFromMessage($message);
        if ($schemaId) {
            return $this->getHandlerBySchemaId($schemaId);
        }
    }

    private function getSchemaIdFromMessage(Message $message)
    {
        $decoded = valueOf(decode($message->payload));
        if (is_array($decoded) && array_key_exists('schemaId', $decoded)) {
            return $decoded['schemaId'];
        }

    }

    public function getHandlers(): array
    {
        return $this->handlers;
    }


}