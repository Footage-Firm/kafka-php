<?php

namespace Tests\Common;

use KafkaPhp\Common\TopicFormatter;
use Tests\BaseTestCase;
use Tests\Util\Fakes\FakeRecord;

class TopicFormatterTest extends BaseTestCase
{

    public function testTopicFromRecord()
    {
        $topic = TopicFormatter::topicFromRecord(new FakeRecord());
        self::assertEquals('fake-record', $topic);
    }

    public function testTopiFromRecordName()
    {
        $fakeRecordName = '';
        $words = $this->faker()->words(3);
        foreach ($words as $word) {
            $fakeRecordName .= ucfirst($word);
        }

        $topic = TopicFormatter::topicFromRecordName($fakeRecordName);
        self::assertEquals(strtolower(implode('-', $words)), $topic);
    }

    public function testProducerFailureTopicFromRecord()
    {
        $originalTopic = 'some-topic';
        $topic = TopicFormatter::producerFailureTopic($originalTopic);
        self::assertEquals(TopicFormatter::INVALID_TOPIC_PREFIX . 'some-topic', $topic);
    }

    public function testConsumerFailureTopicFromRecord()
    {
        $groupId = $this->faker()->word;
        $topic = TopicFormatter::consumerFailureTopic(new FakeRecord(), $groupId);
        self::assertEquals(TopicFormatter::FAILURE_TOPIC_PREFIX . $groupId . '-fake-record', $topic);
    }
}