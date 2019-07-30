<?php

namespace Tests;

use Faker\Factory;
use PHPUnit\Framework\TestCase;

class TestCaseWithFaker extends TestCase
{

    protected $faker;

    public function setUp(): void
    {
        parent::setUp();
        $this->faker = Factory::create();
    }
}

