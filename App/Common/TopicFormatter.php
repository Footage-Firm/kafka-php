<?php

namespace App\Common;

use App\Events\BaseRecord;

class TopicFormatter
{

    public const FAILURE_TOPIC_PREFIX = 'fail-';

    public static function topicFromRecord(BaseRecord $record): string
    {
        $className = Utils::className($record);
        return self::toKebabCase($className);
    }

    public static function topicFromRecordName(string $recordName): string
    {
        return self::toKebabCase($recordName);
    }

    public static function producerFailureTopicFromRecord(BaseRecord $record): string
    {
        return self::FAILURE_TOPIC_PREFIX . Utils::className($record);
    }

    public static function consumerFailureTopicFromRecord(BaseRecord $record, string $groupId)
    {
        return sprintf('%s%s-%s', self::FAILURE_TOPIC_PREFIX, $groupId, Utils::className($record));
    }

    private static function toKebabCase(string $str): string
    {
        $split = preg_split('/(?=[A-Z])/', $str, -1, PREG_SPLIT_NO_EMPTY);
        return strtolower(implode('-', $split));
    }

}