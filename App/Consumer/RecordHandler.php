<?php

namespace App\Consumer;

use EventsPhp\BaseRecord;

class RecordHandler
{

    private $recordType;

    /** @var callable */
    private $handler;

    /** @var callable|null */
    private $failure;

    public function __construct(
      string $recordType,
      callable $handler,
      ?callable $failure
    ) {
        $this->recordType = $recordType;
        $this->handler = $handler;
        $this->failure = $failure;
    }

    public function success(BaseRecord $record): void
    {
        ($this->handler)($record);
    }

    public function fail(BaseRecord $record): void
    {
        if ($this->failure) {
            ($this->failure)($record);
        }
    }

    public function getRecordType(): string
    {
        return $this->recordType;
    }
}