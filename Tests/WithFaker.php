<?php

namespace Tests;

use Faker\Factory;

trait WithFaker
{

    protected $faker;

    public function initFaker(): void
    {
        $this->faker = Factory::create();
    }
}

