<?php

namespace Tests\Fakes;

use App\Events\BaseRecord;
use App\Events\Poc\Common\SharedMeta;

class FakeRecord extends BaseRecord
{

    /** @var SharedMeta */
    private $meta;

    private $id;

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

    public function schema(): string
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "FakeRecord",
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