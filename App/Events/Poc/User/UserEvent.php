<?php

namespace App\Events\Poc\User;

use App\Events\BaseRecord;

use App\Events\Poc\Common\SharedMeta;

class UserEvent extends BaseRecord
{

    /** @var int */
    private $version = 1;

    /** @var SharedMeta */
    private $meta;

    /** @var int */
    private $userId;

    /** @return int */
    public function getVersion(): int
    {
        return $this->version;
    }

    /** @param int $version */
    public function setVersion(int $version): UserEvent
    {
        $this->version = $version;
        return $this;
    }

    /** @return SharedMeta */
    public function getMeta(): SharedMeta
    {
        return $this->meta;
    }

    /** @param SharedMeta $meta */
    public function setMeta(SharedMeta $meta): UserEvent
    {
        $this->meta = $meta;
        return $this;
    }

    /** @return int */
    public function getUserId(): int
    {
        return $this->userId;
    }

    /** @param int $userId */
    public function setUserId(int $userId): UserEvent
    {
        $this->userId = $userId;
        return $this;
    }

    public function jsonSerialize()
    {
        return [
            "version" => $this->encode($this->version),
            "meta" => $this->encode($this->meta),
            "userId" => $this->encode($this->userId),
        ];
    }

    public function schema(): string
    {
        return <<<SCHEMA
{
    "type": "record",
    "name": "UserEvent",
    "namespace": "poc.user",
    "fields": [
        {
            "name": "version",
            "type": "int",
            "default": 1
        },
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
            "name": "userId",
            "type": "int"
        }
    ]
}
SCHEMA;
    }

}