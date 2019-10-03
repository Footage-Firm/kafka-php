<?php

namespace Tests\Fakes;

use EventsPhp\BaseRecord;

class FakeRecordTwo extends BaseRecord
{

    private $id;

    private $name;

    private $version = 1;

    public function setId($id): FakeRecordTwo
    {
        $this->id = $id;
        return $this;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function setName(string $name): FakeRecordTwo
    {
        $this->name = $name;
        return $this;
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