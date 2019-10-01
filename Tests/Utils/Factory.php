<?php


namespace Tests\Utils;


use EventsPhp\Storyblocks\Common\DebugRecord;
use Tests\WithFaker;

class Factory
{
    use WithFaker;

    public static function debugRecord(string $payload = ''): DebugRecord {
        $record = new DebugRecord();
        $record->setPayload($payload);
        return $record;
    }
}