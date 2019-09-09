<?php

namespace Tests\Common;

use App\Common\TopicFormatter;
use Tests\Fakes\FakeRecord;
use Tests\WithFaker;

class TestTopicFormatter
{

    use WithFaker;

    public function testTopicFromRecord()
    {
        $topic = TopicFormatter::topicFromRecord(new FakeRecord());
        self::assertEquals('fake-record', $topic);
    }

    public function testTopiFromRecordName()
    {
        $fakeRecordName = '';
        $words = $this->faker->words(3);
        foreach ($words as $word) {
            $fakeRecordName .= ucfirst($word);
        }

        $topic = TopicFormatter::topicFromRecordName($fakeRecordName);
        self::assertEquals(strtolower(implode('-', $words)), $topic);
    }

    public function testProduverFailureTopicFromRecord()
    {
        $originalTopic = 'some-topic';
        $topic = TopicFormatter::producerFailureTopic($originalTopic);
        self::assertEquals(TopicFormatter::FAILURE_TOPIC_PREFIX . 'some-topic', $topic);
    }

    public function testConsumerFailureTopicFromRecord()
    {
        $groupId = $this->faker->word;
        $topic = TopicFormatter::consumerFailureTopic(new FakeRecord(), $groupId);
        self::assertEquals(TopicFormatter::FAILURE_TOPIC_PREFIX . $groupId . '-FakeRecord', $topic);
    }
}