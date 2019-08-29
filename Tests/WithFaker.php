<?php

namespace Tests;

use Faker\Factory;
use Faker\Generator;

trait WithFaker
{

    /** @var Generator */
    protected $faker;

    public function initFaker(): void
    {
        $this->faker = Factory::create();
    }
}

