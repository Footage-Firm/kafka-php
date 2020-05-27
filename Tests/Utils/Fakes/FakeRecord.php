<?php

namespace Tests\Utils\Fakes;

use EventsPhp\BaseRecord;

class FakeRecord extends BaseRecord
{

    private $id;

    private $version = 1;

    public function setId($id): FakeRecord
    {
        $this->id = $id;
        return $this;
    }

    public function getId(): int
    {
        return $this->id;
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "{$this->name()}",
    "namespace": "testing",
    "fields": [
      { "name": "version", "type": "int", "default": 1},
      { "name": "id", "type": "int" }
    ]
  }
SCHEMA;
    }

    public function jsonSerialize()
    {
        return [
          'id' => $this->encode($this->id),
          'version' => $this->encode($this->version),
        ];
    }
}