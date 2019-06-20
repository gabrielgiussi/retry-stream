This repository contains an Akka Stream for retry event processing.

The stream reads retry events from kafka using [alpakka-kafka](https://github.com/akka/alpakka-kafka) and delegate them to injected processors based on event's metadata.

Records will be retried until they succeed or the number of retries are exhausted.

The key contribution from this implementation consist in taking advantage of alpakka-kafka's ability to backpressure the KafkaConsumer to delay event processing without the need to close the KafkaConsumer or the risk to get OutOfMemory errors or get kicked out by the broker.  
