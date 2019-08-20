<?php

namespace App\Traits;

trait RecordFormatting
{

    public static $SUBJECT_SUFFIX = '-value';

    protected function className($class): string
    {
        $fqClassName = get_class($class);
        $fqClassNameArr = explode('\\', $fqClassName);
        return array_pop($fqClassNameArr);
    }

    protected function kebabCase(string $value): string
    {
        if (!ctype_lower($value)) {
            $value = preg_replace('/\s+/u', '', ucwords($value));

            $value = preg_replace('/(.)(?=[A-Z])/u', '$1' . '-', $value);
            return mb_strtolower($value, 'UTF-8');
        }

        return $value;
    }

    protected function formatAsSubject(string $value): string
    {
        return $this->kebabCase($value) . static::$SUBJECT_SUFFIX;
    }
}