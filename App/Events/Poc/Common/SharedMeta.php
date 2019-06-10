<?php

namespace App\Events\Poc\Common;

use App\Events\BaseRecord;

class SharedMeta extends BaseRecord
{

    /** @var string */
    private $uuid;

    public function subject(): string
    {
        return "shared-meta";
    }

    /** @return string */
    public function getUuid(): string
    {
        return $this->uuid;
    }

    /** @param string $uuid */
    public function setUuid(string $uuid): SharedMeta
    {
        $this->uuid = $uuid;
        return $this;
    }

    public function jsonSerialize()
    {
        return [
          "uuid" => $this->encode($this->uuid),
          "version" => 1
        ];
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
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
SCHEMA;
    }

}