<?php

namespace KafkaPhp\Common;

class ConfigOptions
{
    public const BROKER_LIST = 'metadata.broker.list';
    public const SECURITY_PROTOCOL = 'security.protocol';
    public const CA_PATH = 'ssl.ca.location';
    public const CERT_PATH = 'ssl.certificate.location';
    public const KEY_PATH = 'ssl.key.location';
    public const SASL_MECHANISM = 'sasl.mechanisms';
    public const SASL_USERNAME = 'sasl.username';
    public const SASL_PASSWORD = 'sasl.password';
    // Note: According to the [docs](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md) this is only a Producer config, but the package maintainer [mentions](https://github.com/edenhill/librdkafka/issues/1470#issuecomment-339904446) that it is used for committing offsets.
    public const RETRIES = 'retries';
    public const RETRY_BACKOFF_MS = 'retry.backoff.ms';
    public const SOCKET_TIMEOUT_MS = 'socket.timeout.ms';
    public const HEARTBEAT_INTERVAL_MS = 'heartbeat.interval.ms';
}