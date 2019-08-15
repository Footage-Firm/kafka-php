<?php

namespace Tests\Fakes;

use App\Events\BaseRecord;
use App\Events\Poc\Common\SharedMeta;

class FakeRecord extends BaseRecord
{

    /** @var SharedMeta */
    private $meta;

    private $id;

    private $name;

    private $version = 1;

    public function setMeta(SharedMeta $meta): FakeRecord
    {
        $this->meta = $meta;
        return $this;
    }

    public function setId($id): FakeRecord
    {
        $this->id = $id;
        return $this;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function setName(string $name): void
    {
        $this->name = $name;
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "FakeRecord",
    "namespace": "testing",
    "fields": [
      { "name": "version", "type": "int", "default": 1},
      { "name": "name", "type": "string" },
      { "name": "id", "type": "int" }
    ]
  }
SCHEMA;
    }

    public function jsonSerialize()
    {
        return ['id' => $this->encode($this->id), 'name' => $this->encode($this->name), 'version' => $this->encode(1)];
    }
}