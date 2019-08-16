<?php

namespace App\Common;

use App\Events\BaseRecord;

class TopicFormatter
{

    public const FAILURE_TOPIC_PREFIX = 'fail-';

    public static function topicFromRecord(BaseRecord $record): string
    {
        $className = self::className($record);
        return self::toKebabCase($className);
    }

    public static function topicFromRecordName(string $recordName): string
    {
        return self::toKebabCase($recordName);
    }

    public static function producerFailureTopic(string $originalTopic): string
    {
        return self::FAILURE_TOPIC_PREFIX . $originalTopic;
    }

    public static function consumerFailureTopic(BaseRecord $record, string $groupId)
    {
        return sprintf('%s%s-%s', self::FAILURE_TOPIC_PREFIX, $groupId, self::className($record));
    }

    private static function toKebabCase(string $str): string
    {
        $split = preg_split('/(?=[A-Z])/', $str, -1, PREG_SPLIT_NO_EMPTY);
        return strtolower(implode('-', $split));
    }

    private static function className($class): string
    {
        $fqClassName = get_class($class);
        $fqClassNameArr = explode('\\', $fqClassName);
        return array_pop($fqClassNameArr);
    }
}