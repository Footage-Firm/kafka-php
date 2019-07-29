<?php

namespace Tests\Common;

use App\Common\TopicFormatter;
use Tests\Fakes\FakeRecord;
use Tests\TestCaseWithFaker;

class TestTopicFormatter extends TestCaseWithFaker
{

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
        $topic = TopicFormatter::producerFailureTopicFromRecord(new FakeRecord());
        self::assertEquals(TopicFormatter::FAILURE_TOPIC_PREFIX . 'FakeRecord', $topic);
    }

    public function testConsumerFailureTopicFromRecord()
    {
        $groupId = $this->faker->word;
        $topic = TopicFormatter::consumerFailureTopicFromRecord(new FakeRecord(), $groupId);
        self::assertEquals(TopicFormatter::FAILURE_TOPIC_PREFIX . $groupId . '-FakeRecord', $topic);
    }
}