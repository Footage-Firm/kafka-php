<?php

namespace Tests\Fakes;

use Faker\Factory;

class FakeRecordFactory
{

    public static function fakeRecord(int $id = null, string $name = null): FakeRecord
    {
        $id = $id ?? Factory::create()->numberBetween(0, 9999);
        $name = $name ?? Factory::create()->word;
        $fake = new FakeRecord();
        $fake->setId($id);
        $fake->setName($name);
        return $fake;
    }
}