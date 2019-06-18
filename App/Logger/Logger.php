<?php

namespace App\Logger;

use Monolog\Handler\StreamHandler;
use Monolog\Logger as MonoLogger;
use Psr\Log\AbstractLogger;

class Logger extends AbstractLogger
{

    /** @var MonoLogger|null */
    private $monoLogger;

    public function __construct()
    {
        $this->monoLogger = new MonoLogger('defaultLogger');
        $this->monoLogger->pushHandler(new StreamHandler(__DIR__ . '/app.log', MonoLogger::DEBUG));

    }


    public function log($level, $message, array $context = []): void
    {
        $this->monoLogger->log($level, $message, $context);
    }
}