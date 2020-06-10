<?php

namespace KafkaPhp\Consumer;

use KafkaPhp\Common\TopicFormatter;
use KafkaPhp\Producer\Producer;
use KafkaPhp\Traits\RecordFormatter;
use KafkaPhp\Traits\ShortClassName;
use EventsPhp\BaseRecord;
use Psr\Log\LoggerInterface;
use Throwable;

class RecordProcessor
{

    use RecordFormatter;
    use ShortClassName;

    /** @var RecordHandler[] */
    private $handlers = [];

    private $shouldSendToFailureTopic = true;

    private $groupId;

    /** @var Producer */
    private $failureProducer;

    /** @var LoggerInterface */
    private $logger;

    private $numRetries = ConsumerBuilder::DEFAULT_RETRIES;

    public function __construct(string $groupId, Producer $failureProducer, LoggerInterface $logger)
    {
        $this->groupId = $groupId;
        $this->failureProducer = $failureProducer;
        $this->logger = $logger;
    }

    public function subscribe(string $recordName, callable $success, callable $failure = null): void
    {
        $name = self::shortClassName($recordName);
        $this->handlers[$name] = new RecordHandler(
          $recordName,
          $success,
          $failure
        );
    }

    public function process(array $decoded): void
    {
        $key = key($decoded);
        $handler = $this->handlers[$key] ?? null;

        if ($handler) {
            $record = $this->getRecordFromDecoded($decoded[$key], $handler);
            $this->logger->debug('Handling record.', ['record' => json_encode($record)]);
            try {
                $handler->success($record);
            } catch (Throwable $t) {
                $this->logger->error('Exception thrown in handler.', ['error' => $t, 'record' => json_encode($record)]);
                $this->retry($record, $handler);
            }
        }
    }

    /*
     * Returns an array of handlers with key of the short class name of the subscribed object, and value
     * of a MessageHandler
     */
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

    public function handleFailure(BaseRecord $record, RecordHandler $handler = null): void
    {
        $handler = $handler ?? $this->handlers[$record->name()];
        $handler->fail($record);

        if ($this->shouldSendToFailureTopic) {
            $this->sendToFailureTopic($record);
        }
    }

    private function retry(BaseRecord $record, RecordHandler $handler, int $currentRetry = 1): void
    {
        if ($currentRetry >= $this->numRetries) {
            $this->handleFailure($record, $handler);
        } else {
            $this->logger->notice('RecordProcessor: retrying record.', ['record' => json_encode($record), 'currentRetry' => $currentRetry]);
            try {
                $handler->success($record);
            } catch (Throwable $t) {
                $this->retry($record, $handler, $currentRetry + 1);
            }
        }
    }

    private function sendToFailureTopic(BaseRecord $record): void
    {
        $topic = TopicFormatter::consumerFailureTopic($record, $this->groupId);
        $this->logger->warning('Sending record to failure topic.', ['record' => $record, 'topic' => $topic]);
        $this->failureProducer->produce($record, null, $topic);
    }

    private function getRecordFromDecoded(array $decodedValue, RecordHandler $handler): BaseRecord
    {
        $recordType = $handler->getRecordType();
        /** @var BaseRecord $record */
        $record = new $recordType();
        $record->decode($decodedValue);
        return $record;
    }
}