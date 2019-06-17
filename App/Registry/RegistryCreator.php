<?php
//
//namespace App\Registry;
//
//use FlixTech\SchemaRegistryApi\Registry;
//use FlixTech\SchemaRegistryApi\Registry\Cache\AvroObjectCacheAdapter;
//use FlixTech\SchemaRegistryApi\Registry\CachedRegistry;
//use FlixTech\SchemaRegistryApi\Registry\PromisingRegistry;
//use GuzzleHttp\Client;
//
//class RegistryCreator
//{
//
//    public static function createCachedRegistry(
//      string $schemaRegistryUri,
//      string $username = null,
//      string $password = null
//    ): Registry {
//        $client = new Client(['base_uri' => $schemaRegistryUri, 'auth' => [$username, $password]]);
//        return new CachedRegistry(new PromisingRegistry($client), new AvroObjectCacheAdapter());
//    }
//
//    public static function createAivenCachedRegistry() {
//        return
//    }
//}