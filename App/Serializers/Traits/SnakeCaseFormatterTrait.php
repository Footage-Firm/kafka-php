<?php

namespace App\Serializers\Traits;

// I think this will be reused in a few places, but right now its only used in the serializer.
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