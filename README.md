# kafka-delay-retry

WORK IN PROGRESS

There is no feature for delayed-retry in kafka and even non-blocking retry is not supported.

This is a tool to allow delayed retry (only exponential backoff currently). Messages from retry topics (`*-retry`) are put in database for temporary storage and wait duration is computed (default value or 2x previous wait).
A worker then polls the database to find expired messages and inject then in source topic, with a header for wait duration.

If the main consumer cannot process the message, it publishes in retry topic and keeps the headers.

This repeats until all messages are processed with success by main consumer.
