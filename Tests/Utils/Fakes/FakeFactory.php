<?php

namespace Tests\Utils\Fakes;

use Faker\Factory;

class FakeFactory
{

    public static function fakeRecord(int $id = null): FakeRecord
    {
        $id = $id ?? Factory::create()->numberBetween(0, 9999);
        $fake = new FakeRecord();
        $fake->setId($id);
        return $fake;
    }

    public static function invalidRecord(): FakeRecord
    {
        $fake = new FakeRecord();
        $fake->setId(Factory::create()->word);
        return $fake;
    }

}