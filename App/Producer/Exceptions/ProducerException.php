<?php

namespace App\Producer\Exceptions;

use Exception;
use Throwable;

class ProducerException extends Exception
{

    public function __construct($message = '', $code = 0, Throwable $previous = null)
    {
        parent::__construct('Produce Error: ' . $message, $code, $previous);
    }
}