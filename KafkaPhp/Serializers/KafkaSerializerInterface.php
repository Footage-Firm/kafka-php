<?php

namespace KafkaPhp\Serializers;

use EventsPhp\BaseRecord;

interface KafkaSerializerInterface
{

    public function serialize(BaseRecord $record, string $key): array;

    public function deserialize(string $payload): array;
}