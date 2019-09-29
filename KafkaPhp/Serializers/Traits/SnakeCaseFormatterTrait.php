<?php

namespace KafkaPhp\Serializers\Traits;

trait SnakeCaseFormatterTrait
{

    protected function convertToSnakeCase($value, $delimiter = '_')
    {
        if (!ctype_lower($value)) {
            $value = preg_replace('/\s+/u', '', ucwords($value));

            $value = preg_replace('/(.)(?=[A-Z])/u', '$1' . $delimiter, $value);
            return mb_strtolower($value, 'UTF-8');
        }

        return $value;
    }
}