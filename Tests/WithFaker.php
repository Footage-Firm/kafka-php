<?php

namespace Tests;

use Faker\Factory;
use Faker\Generator;

trait WithFaker
{

    /** @var Generator */
    private $faker;

    protected function faker(): Generator
    {
        if (!$this->faker) {
            $this->faker = Factory::create();
        }
        return $this->faker;
    }
}

