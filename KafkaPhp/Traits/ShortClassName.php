<?php

namespace KafkaPhp\Traits;

trait ShortClassName
{

    public static function shortClassName(string $fqClassName)
    {
        $fqClassNameArr = explode('\\', $fqClassName);
        return array_pop($fqClassNameArr);
    }
}