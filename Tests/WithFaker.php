<?php

namespace Tests;

use Faker\Factory;
use Faker\Generator;

trait WithFaker
{

    /** @var Generator */
    protected $faker;

    /**
     * @deprecated Use the singleton getter below.
     */
    public function initFaker(): void
    {
        $this->faker = Factory::create();
    }

    public function faker(): Generator
    {
        if (!$this->faker) {
            $this->faker = Factory::create();
        }
        return $this->faker;
    }
}

