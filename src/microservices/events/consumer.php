<?php
/**
 * Kafka consumer: reads movie/user/payment events and writes them to the service log.
 */

function logLine(string $message): void
{
    file_put_contents('php://stderr', '[' . date('c') . '] ' . $message . PHP_EOL);
}

$brokers = getenv('KAFKA_BROKERS') ?: 'kafka:9092';
$topics = ['movie-events', 'user-events', 'payment-events'];

$conf = new RdKafka\Conf();
$conf->set('bootstrap.servers', $brokers);
$conf->set('group.id', 'events-service');
$conf->set('auto.offset.reset', 'earliest');
$conf->set('enable.auto.commit', 'true');
// Topics may not exist yet while Kafka is starting up
$conf->set('allow.auto.create.topics', 'true');
$conf->set('topic.metadata.refresh.interval.ms', '5000');

$consumer = new RdKafka\KafkaConsumer($conf);
$consumer->subscribe($topics);
logLine('[consumer] subscribed to ' . implode(', ', $topics) . " (brokers: {$brokers})");

while (true) {
    $message = $consumer->consume(1000);
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $event = json_decode($message->payload, true);
            logLine(sprintf('[consumer] processed %s event id=%s topic=%s partition=%d offset=%d payload=%s',
                $event['type'] ?? 'unknown',
                $event['id'] ?? '-',
                $message->topic_name,
                $message->partition,
                $message->offset,
                json_encode($event['payload'] ?? null)
            ));
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            break;
        case RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART:
            // topics are created by Kafka a bit later, metadata is refreshed automatically
            logLine('[consumer] waiting for topics: ' . $message->errstr());
            sleep(5);
            break;
        default:
            logLine('[consumer] error: ' . $message->errstr());
            sleep(2);
            break;
    }
}
