<?php
/**
 * Proxy service (API Gateway) for CinemaAbyss.
 * Implements the Strangler Fig pattern: /api/movies traffic is gradually moved
 * from the monolith to the movies microservice according to a feature flag.
 */

function env(string $key, string $default): string
{
    $value = getenv($key);
    return ($value === false || $value === '') ? $default : $value;
}

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

$monolithUrl = rtrim(env('MONOLITH_URL', 'http://monolith:8080'), '/');
$moviesUrl = rtrim(env('MOVIES_SERVICE_URL', 'http://movies-service:8081'), '/');
$eventsUrl = rtrim(env('EVENTS_SERVICE_URL', 'http://events-service:8082'), '/');
$gradualMigration = strtolower(env('GRADUAL_MIGRATION', 'false')) === 'true';
$migrationPercent = max(0, min(100, (int) env('MOVIES_MIGRATION_PERCENT', '0')));

$method = $_SERVER['REQUEST_METHOD'];
$uri = $_SERVER['REQUEST_URI'];
$path = parse_url($uri, PHP_URL_PATH);

if ($path === '/health') {
    sendJson(200, ['status' => true]);
    return;
}

// Routing
if ($path === '/api/movies' || str_starts_with($path, '/api/movies/')) {
    // Feature flag: with GRADUAL_MIGRATION=false everything stays on the monolith
    $toMicroservice = $gradualMigration && random_int(1, 100) <= $migrationPercent;
    $target = $toMicroservice ? $moviesUrl : $monolithUrl;
    $name = $toMicroservice ? 'movies-service' : 'monolith';
} elseif ($path === '/api/events' || str_starts_with($path, '/api/events/')) {
    $target = $eventsUrl;
    $name = 'events-service';
} else {
    // users, payments, subscriptions, ... are still served by the monolith
    $target = $monolithUrl;
    $name = 'monolith';
}

logLine("{$method} {$uri} -> {$name}");

// Forward the request
$headers = [];
foreach (getallheaders() as $key => $value) {
    $lower = strtolower($key);
    if (in_array($lower, ['host', 'content-length', 'connection'], true)) {
        continue;
    }
    $headers[] = "{$key}: {$value}";
}

$ch = curl_init($target . $uri);
curl_setopt_array($ch, [
    CURLOPT_CUSTOMREQUEST => $method,
    CURLOPT_HTTPHEADER => $headers,
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_HEADER => true,
    CURLOPT_CONNECTTIMEOUT => 5,
    CURLOPT_TIMEOUT => 30,
]);
$body = file_get_contents('php://input');
if ($body !== '' && $body !== false) {
    curl_setopt($ch, CURLOPT_POSTFIELDS, $body);
}

$response = curl_exec($ch);
if ($response === false) {
    logLine("Error proxying to {$name}: " . curl_error($ch));
    sendJson(502, ['error' => 'Bad Gateway']);
    return;
}

$status = curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
$headerSize = curl_getinfo($ch, CURLINFO_HEADER_SIZE);
curl_close($ch);

http_response_code($status);
foreach (explode("\r\n", substr($response, 0, $headerSize)) as $line) {
    if (strpos($line, ':') === false) {
        continue;
    }
    [$key] = explode(':', $line, 2);
    if (in_array(strtolower(trim($key)), ['transfer-encoding', 'connection', 'content-length'], true)) {
        continue;
    }
    header($line, false);
}
header("X-Proxy-Target: {$name}");
echo substr($response, $headerSize);
