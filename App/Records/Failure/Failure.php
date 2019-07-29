<?php

namespace App\Records\Failure;

use App\Events\BaseRecord;
use App\Events\Poc\Common\SharedMeta;

class Failure extends BaseRecord
{

    /** @var SharedMeta */
    private $meta;

    private $payload;

    private $failedTopic;

    private $details;

    public function getMeta(): SharedMeta
    {
        return $this->meta;
    }

    public function setMeta(SharedMeta $meta): Failure
    {
        $this->meta = $meta;
        return $this;
    }

    public function getPayload(): string
    {
        return $this->payload;
    }

    public function setPayload(string $payload): Failure
    {
        $this->payload = $payload;
        return $this;
    }

    public function getFailedTopic(): string
    {
        return $this->failedTopic;
    }

    public function setFailedTopic(string $failedTopic): Failure
    {
        $this->failedTopic = $failedTopic;
        return $this;
    }

    public function getDetails(): string
    {
        return $this->details;
    }

    public function setDetails(string $details): Failure
    {
        $this->details = $details;
        return $this;
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
    type: "record",
    name: "Failure",
    namespace: "poc",
    fields: [
      { name: "payload", type: "string" },
      { name: "failedTopic", type: "string"},
      { name: "details", type: "string", default: ""}
    ]
  };
SCHEMA;
    }

    public function jsonSerialize()
    {
        return [
          'payload' => $this->encode($this->payload),
          'failedTopic' => $this->encode($this->failedTopic),
          'details' => $this->encode($this->details),
        ];
    }
}