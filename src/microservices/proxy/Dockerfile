FROM php:8.3-cli-alpine

WORKDIR /app

COPY index.php .

# Several workers so the built-in server can handle parallel requests
ENV PHP_CLI_SERVER_WORKERS=4
ENV PORT=8000

EXPOSE 8000

CMD ["sh", "-c", "php -S 0.0.0.0:${PORT} index.php"]
