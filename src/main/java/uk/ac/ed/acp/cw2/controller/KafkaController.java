package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StopWatch;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * KafkaController is a REST API controller used to interact with Apache Kafka for producing
 * and consuming stock symbol events. This class provides endpoints for sending stock symbols
 * to a Kafka topic and retrieving stock symbols from a Kafka topic.
 * <p>
 * It is designed to handle dynamic Kafka configurations based on the runtime environment
 * and supports security configurations such as SASL and JAAS.
 */
@RestController()
@RequestMapping("/kafka")
public class KafkaController {

    private static final Logger logger = LoggerFactory.getLogger(KafkaController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    public KafkaController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

    /**
     * Constructs Kafka properties required for KafkaProducer and KafkaConsumer configuration.
     *
     * @param environment the runtime environment providing dynamic configuration details
     *                     such as Kafka bootstrap servers.
     * @return a Properties object containing configuration properties for Kafka operations.
     */
    private Properties getKafkaProperties(RuntimeEnvironment environment) {
        Properties kafkaProps = new Properties();
        kafkaProps.put("bootstrap.servers", environment.getKafkaBootstrapServers());
        kafkaProps.put("acks", "all");
        kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        kafkaProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProps.setProperty("enable.auto.commit", "true");
        kafkaProps.put("acks", "all");

        kafkaProps.put("group.id", UUID.randomUUID().toString());
        kafkaProps.setProperty("auto.offset.reset", "earliest");
        kafkaProps.setProperty("enable.auto.commit", "true");

        if (environment.getKafkaSecurityProtocol() != null) {
            kafkaProps.put("security.protocol", environment.getKafkaSecurityProtocol());
        }
        if (environment.getKafkaSaslMechanism() != null) {
            kafkaProps.put("sasl.mechanism", environment.getKafkaSaslMechanism());
        }
        if (environment.getKafkaSaslJaasConfig() != null) {
            kafkaProps.put("sasl.jaas.config", environment.getKafkaSaslJaasConfig());
        }

        return kafkaProps;
    }

    @PutMapping("/{writeTopic}/{messageCount}")
    public ResponseEntity<String> sendJsonMessages(@PathVariable String writeTopic, @PathVariable int messageCount) {
        logger.info(String.format("Writing %d messages in topic %s", messageCount, writeTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        String studentId = "s2677989";

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < messageCount; i++) {
//                final String key = stockSymbols[new Random().nextInt(stockSymbols.length)];
//                final String value = String.valueOf(i);

                String key = "omittedKey";
                String json = String.format("{\"uid\":\"%s\", \"counter\":%d}", studentId, i);

                producer.send(new ProducerRecord<>(writeTopic, json), (recordMetadata, ex) -> {
                    if (ex != null)
                        ex.printStackTrace();
                    else
                        logger.info(String.format("Produced event to topic %s: key = %-10s value = %s%n", writeTopic, key, json));
                }).get(1000, TimeUnit.MILLISECONDS);
            }
            logger.info(String.format("%d record(s) sent to Kafka\n", messageCount));
            return ResponseEntity.ok("Sent " + messageCount + " messages to Kafka topic " + writeTopic);
        } catch (ExecutionException e) {
            logger.error("execution exc: " + e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka message send failed: " + e.getMessage());
        } catch (TimeoutException e) {
            logger.error("timeout exc: " + e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka message send failed: " + e.getMessage());
        } catch (InterruptedException e) {
            logger.error("interrupted exc: " + e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka message send failed: " + e.getMessage());
        }
    }

    @GetMapping("/{readTopic}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveJsonMessages(@PathVariable String readTopic, @PathVariable int timeoutInMsec) {
        logger.info(String.format("Reading messages from topic %s", readTopic));
        Properties kafkaProps = getKafkaProperties(environment);

        List<String> result = new ArrayList<>();

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(timeoutInMsec));
            for (ConsumerRecord<String, String> record : records) {
                logger.info(String.format("[%s] %s: %s %s %s %s", record.topic(), record.key(), record.value(), record.partition(), record.offset(), record.timestamp()));
                result.add(record.value());
            }
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            logger.error("Kafka read failed", e);
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Kafka consume failed: " + e.getMessage());
        }

    }



    @PostMapping("/testMessage")
    public ResponseEntity<String> sendTestProcessMessages(@RequestParam String topic) {
        Properties kafkaProps = getKafkaProperties(environment);
        String studentId = "s2677989";

        List<String> testKeys = List.of("AAA", "ABCD", "ABCDE","1111","33333");  // good, good, bad ,good, bad
        double[] values = {10.5, 11.0, 10.0, 4.0, 99.0};

        try (var producer = new KafkaProducer<String, String>(kafkaProps)) {
            for (int i = 0; i < testKeys.size(); i++) {
                Map<String, Object> msg = new HashMap<>();
                msg.put("uid", studentId);
                msg.put("key", testKeys.get(i));
                msg.put("comment", "test comment");
                msg.put("value", values[i]);

                String json = new ObjectMapper().writeValueAsString(msg);

                producer.send(new ProducerRecord<>(topic, null, json)).get(1000, TimeUnit.MILLISECONDS);
                logger.info(" Test message sent to {}: {}", topic, json);
            }
        } catch (Exception e) {
            logger.error(" Failed to send test message", e);
            return ResponseEntity.status(500).body("Failed");
        }

        return ResponseEntity.ok("Test messages sent to Kafka");
    }
}
