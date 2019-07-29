<?php

namespace App\Common;

class Utils
{

    /**
     * Get the class name without the namespace.
     *
     * @param $class
     *
     * @return string
     */
    public static function className($class): string
    {
        $fqClassName = get_class($class);
        $fqClassNameArr = explode('\\', $fqClassName);
        return array_pop($fqClassNameArr);
    }
}