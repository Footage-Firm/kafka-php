<?php

namespace KafkaPhp\Common;

class ConfigOptions
{

    public const SSL = 'ssl';

    public const BROKER_LIST = 'metadata.broker.list';

    public const SECURITY_PROTOCOL = 'security.protocol';

    public const CA_PATH = 'ssl.ca.location';

    public const CERT_PATH = 'ssl.certificate.location';

    public const KEY_PATH = 'ssl.key.location';

    public const RETRIES = 'retries';

    public const RETRY_BACKOFF_MS = 'retry.backoff.ms';
}