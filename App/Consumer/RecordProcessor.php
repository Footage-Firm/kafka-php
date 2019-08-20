<?php

namespace App\Consumer;

use App\Common\TopicFormatter;
use App\Events\BaseRecord;
use App\Producer\Producer;
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

    public const DEFAULT_RETRIES = 3;

    /** @var MessageHandler[] */
    private $handlers = [];

    private $registry;

    private $serializer;

    private $numRetries = self::DEFAULT_RETRIES;

    private $shouldSendToFailureTopic = true;

    private $groupId;

    private $failureProducer;

    public function __construct(
      Registry $registry,
      KafkaSerializerInterface $serializer,
      string $groupId,
      Producer $failureProducer
    ) {
        $this->registry = $registry;
        $this->serializer = $serializer;
        $this->groupdId = $groupId;
        $this->failureProducer = $failureProducer;
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
            $record = $this->serializer->deserialize($message->payload, $handler->getRecord());
            try {
                return $handler->success($record);
            } catch (Throwable $t) {
                return $this->retry($record);
            }
        }
    }

    private function retry(BaseRecord $record, MessageHandler $handler, int $currentTry = 0)
    {
        if ($currentTry >= $this->numRetries) {
            $this->handleFailure($handler, $record);
        } else {
            try {
                return $handler->success($record);
            } catch (Throwable $t) {
                $this->retry($record, $handler, $currentTry + 1);
            }
        }
    }

    private function handleFailure(MessageHandler $handler, BaseRecord $record)
    {
        $handler->fail($record);
        if ($this->shouldSendToFailureTopic) {
            $this->sendToFailureTopic($record);
        }
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
        $handler = null;
        foreach ($this->handlers as $i => $handler) {
            if ($handler->getSchemaId() === $schemaId) {
                $handler = $this->handlers[$i];
                break;
            }
        }

        return $handler;
    }

    private function getHandlerForMessage(Message $message): ?MessageHandler
    {
        $schemaId = $this->getSchemaIdFromMessage($message);
        return $schemaId ? $this->getHandlerBySchemaId($schemaId) : null;
    }

    private function getSchemaIdFromMessage(Message $message)
    {
        $decoded = valueOf(decode($message->payload));
        return is_array($decoded) && array_key_exists('schemaId', $decoded)
          ? $decoded['schemaId']
          : null;

    }

    private function sendToFailureTopic(BaseRecord $record): void
    {
        $topic = TopicFormatter::consumerFailureTopic($record, $this->groupId);
        $this->failureProducer->produce($record, $topic);
    }

    public function getHandlers(): array
    {
        return $this->handlers;
    }


}