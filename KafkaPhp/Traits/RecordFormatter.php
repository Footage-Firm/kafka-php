<?php

namespace KafkaPhp\Traits;

trait RecordFormatter
{

    protected function kebabCase(string $value): string
    {
        if (!ctype_lower($value)) {
            $value = preg_replace('/\s+/u', '', ucwords($value));

            $value = preg_replace('/(.)(?=[A-Z])/u', '$1' . '-', $value);
            return mb_strtolower($value, 'UTF-8');
        }

        return $value;
    }
}