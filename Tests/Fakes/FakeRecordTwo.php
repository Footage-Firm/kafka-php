<?php

namespace Tests\Fakes;

use App\Events\BaseRecord;
use App\Events\Poc\Common\SharedMeta;

class FakeRecordTwo extends BaseRecord
{

    /** @var SharedMeta */
    private $meta;

    private $id;

    public function setMeta(SharedMeta $meta): FakeRecordTwo
    {
        $this->meta = $meta;
        return $this;
    }

    public function setId(int $id): FakeRecordTwo
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
    "name": "FakeRecordTwo",
    "namespace": "test",
    "fields": [
        {
            "name": "meta",
            "type": {
                "type": "record",
                "name": "SharedMeta",
                "namespace": "poc.common",
                "fields": [
                    {
                        "name": "version",
                        "type": "int",
                        "default": 1
                    },
                    {
                        "name": "uuid",
                        "type": "string"
                    }
                ]
            }
        },
        {
            "name": "id",
            "type": "int"
        }
    ]
}
SCHEMA;
    }

    public function jsonSerialize()
    {
        return ['id' => $this->encode($this->id)];
    }
}