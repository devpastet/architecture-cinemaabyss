#!/bin/sh
# Consumer runs in the background and is restarted if it exits (e.g. Kafka not ready yet)
(
  while true; do
    php /app/consumer.php
    echo "[consumer] exited, restarting in 5s" >&2
    sleep 5
  done
) &

exec php -S 0.0.0.0:${PORT} /app/index.php
