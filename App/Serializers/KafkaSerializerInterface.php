<?php

namespace App\Serializers;

use EventsPhp\BaseRecord;

interface KafkaSerializerInterface
{

    public function serialize(BaseRecord $record);

    public function deserialize(string $payload);
}