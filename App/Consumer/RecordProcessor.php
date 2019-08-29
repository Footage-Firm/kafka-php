<?php

namespace App\Consumer;

use App\Common\TopicFormatter;
use App\Events\BaseRecord;
use App\Producer\Producer;
use App\Traits\RecordFormatter;
use App\Traits\ShortClassName;
use Throwable;

class RecordProcessor
{

    use RecordFormatter;
    use ShortClassName;

    /** @var MessageHandler[] */
    private $handlers = [];

    private $shouldSendToFailureTopic = true;

    private $groupId;

    private $failureProducer;

    private $numRetries = ConsumerBuilder::DEFAULT_RETRIES;

    public function __construct(string $groupId, Producer $failureProducer)
    {
        $this->groupId = $groupId;
        $this->failureProducer = $failureProducer;
    }

    public function subscribe(string $recordName, callable $success, callable $failure = null): void
    {
        $name = self::shortClassName($recordName);
        $this->handlers[$name] = new MessageHandler(
          $recordName,
          $success,
          $failure
        );
    }

    public function process(array $decoded): void
    {
        $key = key($decoded);
        $handler = $this->handlers[$key];


        if ($handler) {
            $record = $this->getRecordFromDecoded($decoded[$key], $handler);
            try {
                $handler->success($record);
            } catch (Throwable $t) {
                $this->retry($record, $handler);
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

    public function handleFailure(BaseRecord $record, MessageHandler $handler = null): void
    {
        $handler = $handler ?? $this->handlers[$record->name()];
        $handler->fail($record);

        if ($this->shouldSendToFailureTopic) {
            $this->sendToFailureTopic($record);
        }
    }

    private function retry(BaseRecord $record, MessageHandler $handler, int $currentTry = 0): void
    {
        if ($currentTry >= $this->numRetries) {
            $this->handleFailure($record, $handler);
        } else {
            try {
                $handler->success($record);
            } catch (Throwable $t) {
                $this->retry($record, $handler, $currentTry + 1);
            }
        }
    }


    private function sendToFailureTopic(BaseRecord $record): void
    {
        $topic = TopicFormatter::consumerFailureTopic($record, $this->groupId);
        $this->failureProducer->produce($record, $topic);
    }

    private function getRecordFromDecoded(array $decodedValue, MessageHandler $handler): BaseRecord
    {
        $recordType = $handler->getRecordType();
        /** @var BaseRecord $record */
        $record = new $recordType();
        $record->decode($decodedValue);
        return $record;

    }
}