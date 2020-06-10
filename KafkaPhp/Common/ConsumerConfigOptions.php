<?php

namespace KafkaPhp\Common;

class ConsumerConfigOptions
{

    public const AUTO_COMMIT = 'enable.auto.commit';

    public const AUTO_COMMIT_INTERVAL = 'auto.commit.interval.ms';

    public const ENABLE_AUTO_OFFSET_STORE = 'enable.auto.offset.store';

    public const GROUP_ID = 'group.id';

    public const AUTO_OFFSET_RESET = 'auto.offset.reset';

    public const OFFSET_STORE_METHOD = 'offset.store.method';

    public const TOPIC_METADATA_REFRESH_SPARSE = 'topic.metadata.refresh.sparse';

}