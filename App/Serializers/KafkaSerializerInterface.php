<?php

namespace App\Serializers;

use App\Events\BaseRecord;
use RdKafka\Message;

interface KafkaSerializerInterface
{

    public function serialize(BaseRecord $record);

    public function deserialize(Message $message, BaseRecord $record);
}