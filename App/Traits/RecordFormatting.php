<?php

namespace App\Traits;

trait RecordFormatting
{

    protected function className($class): string
    {
        $fqClassName = get_class($class);
        $fqClassNameArr = explode('\\', $fqClassName);
        return array_pop($fqClassNameArr);
    }

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