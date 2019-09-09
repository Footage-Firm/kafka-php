<?php

namespace App\Events;

use JsonSerializable;
use ReflectionClass;
use ReflectionProperty;

abstract class BaseRecord implements JsonSerializable
{

    private $key;

    private $shortName;

    abstract public function schema(): string;

    public function name(): string
    {
        if (!$this->shortName) {
            $reflect = new ReflectionClass($this);
            $this->shortName = $reflect->getShortName();
        }

        return $this->shortName;
    }

    public function setKey(?string $key)
    {
        $this->key = $key;
        return $this;
    }

    public function getKey(): ?string
    {
        return $this->key;
    }

    public function data(): array
    {
        return $this->encode($this);
    }

    protected function encode($mixed)
    {
        return json_decode(json_encode($mixed), true);
    }

    public function decode(array $array)
    {
        $refl = new ReflectionClass($this);

        foreach ($array as $propertyToSet => $value) {

            try {
                $prop = $refl->getProperty($propertyToSet);
            } catch (\ReflectionException $e) {
                $this->{$propertyToSet} = $value;
                continue;
            }

            if ($prop instanceof ReflectionProperty) {
                $prop->setAccessible(true);
                $prop->setValue($this, $value);
            }
        }
    }
}