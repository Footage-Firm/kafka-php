<?php

namespace App\Common;

use App\Events\BaseRecord;
use App\Traits\ShortClassName;

class TopicFormatter
{

    use ShortClassName;

    public const FAILURE_TOPIC_PREFIX = 'fail-';

    public static function topicFromRecord(BaseRecord $record): string
    {
        $className = self::shortClassName($record);
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
        return sprintf('%s%s-%s', self::FAILURE_TOPIC_PREFIX, $groupId,
          self::toKebabCase(self::shortClassName($record)));
    }

    private static function toKebabCase(string $str): string
    {
        $split = preg_split('/(?=[A-Z])/', $str, -1, PREG_SPLIT_NO_EMPTY);
        return strtolower(implode('-', $split));
    }


}