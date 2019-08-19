<?php

namespace App\Consumer;

use App\Events\BaseRecord;
use App\Traits\RecordFormatting;
use AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use GuzzleHttp\Promise\PromiseInterface;
use RdKafka\Message;

class RecordProcessor
{

    use RecordFormatting;

    /** @var MessageHandler[] */
    private $handlers = [];

    /** @var \FlixTech\SchemaRegistryApi\Registry */
    private $registry;

    public function __construct(Registry $registry)
    {
        $this->registry = $registry;
    }

    public function subscribe(BaseRecord $record, callable $success, ?callable $failure)
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
        $this->getHandlerForMessage($message);
        // get correct handler, try to process, retry if errors
    }

    private function getSchemaId(BaseRecord $record): int
    {
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
    }

    public function getHandlers()
    {
        return $this->handlers;
    }

    private function getHandlerForMessage(Message $message)
    {

    }

}