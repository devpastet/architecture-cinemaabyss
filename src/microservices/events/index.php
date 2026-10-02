<?php
/**
 * Events service (Kafka MVP).
 * HTTP API creates movie/user/payment events and publishes them to Kafka (producer).
 * The events are read back and logged by consumer.php.
 */

const TOPICS = [
    'movie' => 'movie-events',
    'user' => 'user-events',
    'payment' => 'payment-events',
];

// Fields used as the event key / id, per event type
const KEY_FIELDS = [
    'movie' => 'movie_id',
    'user' => 'user_id',
    'payment' => 'payment_id',
];

function logLine(string $message): void
{
    file_put_contents('php://stderr', '[' . date('c') . '] ' . $message . PHP_EOL);
}

function sendJson(int $status, array $data): void
{
    http_response_code($status);
    header('Content-Type: application/json');
    echo json_encode($data);
}

function produce(string $type, array $payload): array
{
    $brokers = getenv('KAFKA_BROKERS') ?: 'kafka:9092';
    $topicName = TOPICS[$type];
    $key = (string) ($payload[KEY_FIELDS[$type]] ?? '0');

    $event = [
        'id' => sprintf('%s-%s-%s', $type, $key, bin2hex(random_bytes(4))),
        'type' => $type,
        'timestamp' => gmdate('Y-m-d\TH:i:s\Z'),
        'payload' => $payload,
    ];

    $conf = new RdKafka\Conf();
    $conf->set('bootstrap.servers', $brokers);
    $conf->set('client.id', 'events-service');
    $conf->set('acks', 'all');
    $conf->set('message.timeout.ms', '10000');

    // Delivery report gives us the partition/offset of the written message
    $delivery = null;
    $conf->setDrMsgCb(function ($kafka, $message) use (&$delivery) {
        $delivery = $message;
    });

    $producer = new RdKafka\Producer($conf);
    $topic = $producer->newTopic($topicName);
    $topic->produce(RD_KAFKA_PARTITION_UA, 0, json_encode($event), $key);

    $result = $producer->flush(10000);
    if ($result !== RD_KAFKA_RESP_ERR_NO_ERROR || $delivery === null) {
        throw new RuntimeException('Failed to flush message to Kafka: ' . rd_kafka_err2str($result));
    }
    if ($delivery->err !== RD_KAFKA_RESP_ERR_NO_ERROR) {
        throw new RuntimeException('Kafka delivery error: ' . $delivery->errstr());
    }

    logLine(sprintf('[producer] sent %s event id=%s topic=%s partition=%d offset=%d',
        $type, $event['id'], $topicName, $delivery->partition, $delivery->offset));

    return [
        'status' => 'success',
        'partition' => $delivery->partition,
        'offset' => $delivery->offset,
        'event' => $event,
    ];
}

$method = $_SERVER['REQUEST_METHOD'];
$path = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);

if ($path === '/api/events/health') {
    sendJson(200, ['status' => true]);
    return;
}

if (!preg_match('#^/api/events/(movie|user|payment)/?$#', $path, $matches)) {
    sendJson(404, ['error' => 'Not found']);
    return;
}

if ($method !== 'POST') {
    sendJson(405, ['error' => 'Method not allowed']);
    return;
}

$payload = json_decode(file_get_contents('php://input'), true);
if (!is_array($payload)) {
    sendJson(400, ['error' => 'Invalid JSON body']);
    return;
}

try {
    sendJson(201, produce($matches[1], $payload));
} catch (Throwable $e) {
    logLine('[producer] ' . $e->getMessage());
    sendJson(500, ['error' => $e->getMessage()]);
}
