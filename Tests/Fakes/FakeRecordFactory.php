<?php

namespace Tests\Fakes;

use Faker\Factory;

class FakeRecordFactory
{

    public static function fakeRecord(int $id = null): FakeRecord
    {
        $id = $id ?? Factory::create()->numberBetween();
        $fake = new FakeRecord();
        $fake->setId($id);
        return $fake;
    }

    public static function fakeRecordTwo(int $id = null): FakeRecordTwo
    {
        $id = $id ?? Factory::create()->numberBetween();
        $fake = new FakeRecordTwo();
        $fake->setId($id);
        return $fake;
    }
}