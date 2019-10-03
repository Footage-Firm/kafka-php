<?php


namespace Tests;


use Dotenv\Dotenv;
use PHPUnit\Framework\TestCase;

abstract class BaseTest extends TestCase
{
    use WithFaker;

    protected function setUp(): void
    {
        $dotenv = Dotenv::create(__DIR__ . '/../');
        $dotenv->load();
    }

}