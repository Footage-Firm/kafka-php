<?php

namespace App\Consumer;

use App\Common\TopicFormatter;
use App\Events\BaseRecord;
use App\Producer\Producer;
use App\Serializers\KafkaSerializerInterface;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use AvroSchema;
use FlixTech\SchemaRegistryApi\Registry;
use GuzzleHttp\Promise\PromiseInterface;
use RdKafka\Message;
use Throwable;
use function FlixTech\AvroSerializer\Protocol\decode;
use function Widmogrod\Functional\valueOf;

class RecordProcessor
{

    use RecordFormatter;
    use ShortClassName;

    /** @var MessageHandler[] */
    private $handlers = [];

    private $registry;

    private $serializer;

    private $shouldSendToFailureTopic = true;

    private $groupId;

    private $failureProducer;

    private $numRetries = ConsumerBuilder::DEFAULT_RETRIES;

    public function __construct(
      Registry $registry,
      KafkaSerializerInterface $serializer,
      string $groupId,
      Producer $failureProducer
    ) {
        $this->registry = $registry;
        $this->serializer = $serializer;
        $this->groupId = $groupId;
        $this->failureProducer = $failureProducer;
    }

    public function subscribe(
      BaseRecord $record,
      callable $success,
      callable $failure = null
    ): void {
        $this->handlers[self::shortClassName($record)] = new MessageHandler(
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
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $this->success($message, $record, $handler);
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    // NOP. If there are no new messages in the subscribed topic then a message with this error will be
                    // sent after the consume timeout
                    break;
                default:
                    $this->failure($record, $handler);
                    break;
            }
        }
    }

    public function getHandlers(): array
    {
        return $this->handlers;
    }

    public function setShouldSendToFailureTopic(bool $shouldSendToFailureTopic): self
    {
        $this->shouldSendToFailureTopic = $shouldSendToFailureTopic;
        return $this;
    }

    public function setNumRetries(int $numRetries): self
    {
        $this->numRetries = $numRetries;
        return $this;
    }

    private function success(Message $message, BaseRecord $record, MessageHandler $handler)
    {
        try {
            $handler->success($record);
        } catch (Throwable $t) {
            $this->retry($record, $handler);
        }
    }

    private function retry(BaseRecord $record, MessageHandler $handler, int $currentTry = 0)
    {
        if ($currentTry >= $this->numRetries) {
            $this->failure($record, $handler);
        } else {
            try {
                return $handler->success($record);
            } catch (Throwable $t) {
                $this->retry($record, $handler, $currentTry + 1);
            }
        }
    }

    public function failure(BaseRecord $record, MessageHandler $handler)
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
        $matchingHandler = null;
        foreach ($this->handlers as $i => $handler) {
            if ($handler->getSchemaId() === $schemaId) {
                $matchingHandler = $this->handlers[$i];
                break;
            }
        }

        return $matchingHandler;
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
}