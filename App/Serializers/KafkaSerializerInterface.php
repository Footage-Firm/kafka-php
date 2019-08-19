<?php

namespace App\Serializers;

use App\Events\BaseRecord;

interface KafkaSerializerInterface
{

    public function serialize(BaseRecord $record);

    public function deserialize(string $payload, BaseRecord $record = null);
}