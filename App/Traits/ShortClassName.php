<?php

namespace App\Traits;

trait ShortClassName
{

    public static function shortClassName(object $obj)
    {
        $fqClassNameArr = explode('\\', get_class($obj));
        return array_pop($fqClassNameArr);
    }
}