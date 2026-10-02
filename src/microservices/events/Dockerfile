FROM php:8.3-cli

RUN apt-get update \
    && apt-get install -y --no-install-recommends librdkafka-dev \
    && pecl install rdkafka \
    && docker-php-ext-enable rdkafka \
    && rm -rf /var/lib/apt/lists/* /tmp/pear

WORKDIR /app

COPY index.php consumer.php entrypoint.sh ./
RUN chmod +x entrypoint.sh

ENV PHP_CLI_SERVER_WORKERS=4
ENV PORT=8082

EXPOSE 8082

CMD ["/app/entrypoint.sh"]
