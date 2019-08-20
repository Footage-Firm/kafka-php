<?php

namespace App\Consumer;

use App\Events\BaseRecord;

class MessageHandler
{

    /** @var \App\Events\BaseRecord */
    private $record;

    /** @var callable */
    private $handler;

    /** @var callable|null */
    private $failure;

    /** @var int */
    public $schemaId;

    public function __construct(BaseRecord $record, callable $handler, int $schemaId, ?callable $failure)
    {
        $this->record = $record;
        $this->handler = $handler;
        $this->failure = $failure;
        $this->schemaId = $schemaId;
    }

    public function success(BaseRecord $record)
    {
        return ($this->handler)($record);
    }

    public function fail(BaseRecord $record)
    {
        if ($this->failure) {
            ($this->failure)($record);
        }
    }

    public function getRecord(): BaseRecord
    {
        return $this->record;
    }

    public function getSchemaId(): int
    {
        return $this->schemaId;
    }

}