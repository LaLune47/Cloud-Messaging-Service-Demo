package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.server.ResponseStatusException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;

/**
 * Controller class that handles various HTTP endpoints for the application.
 * Provides functionality for serving the index page, retrieving a static UUID,
 * and managing key-value pairs through POST requests.
 */
@RestController()
public class ServiceController {

    private static final Logger logger = LoggerFactory.getLogger(ServiceController.class);
    private final RuntimeEnvironment environment;
    private final ObjectMapper mapper = new ObjectMapper();

    public ServiceController(RuntimeEnvironment environment) {
        this.environment = environment;
    }

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


    @GetMapping("/")
    public String index() {
        StringBuilder currentEnv = new StringBuilder();
        currentEnv.append("<ul>");
        System.getenv().keySet().forEach(key -> currentEnv.append("<li>").append(key).append(" = ").append(System.getenv(key)).append("</li>"));
        currentEnv.append("</ul>");

        return "<html><body>" +
                "<h1>Welcome from ACP CW2</h1>" +
                "<h2>Environment variables </br><div> " + currentEnv.toString() + "</div></h2>" +
                "</body></html>";
    }

    @GetMapping("/uuid")
    public String uuid() {
        return "s12345678";
    }

    @PostMapping("/processMessages")
    public ResponseEntity<String> processMessages(@RequestBody Map<String, Object> requestBody) {
        String readTopic = (String) requestBody.get("readTopic");
        String writeQueueGood = (String) requestBody.get("writeQueueGood");
        String writeQueueBad = (String) requestBody.get("writeQueueBad");
        int messageCount = (int) requestBody.get("messageCount");

        logger.info("Start processing messages from topic '{}'", readTopic);
        Properties kafkaProps = getKafkaProperties(environment);

        List<Map<String, Object>> goodMessages = new ArrayList<>();
        List<Map<String, Object>> badMessages = new ArrayList<>();

        try (var consumer = new KafkaConsumer<String, String>(kafkaProps)) {
            consumer.subscribe(Collections.singletonList(readTopic));

            int collected = 0;
            long startTime = System.currentTimeMillis();

            while (collected < messageCount) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));

                for (ConsumerRecord<String, String> record : records) {
                    String json = record.value();
                    logger.info("Received raw json-style message: {}", json);

//                    ObjectMapper mapper = new ObjectMapper();
                    Map<String, Object> message = mapper.readValue(json, Map.class);

                    String key = (String) message.get("key");

                    if (key.length() == 3 || key.length() == 4) {
                        goodMessages.add(message);
                    } else if (key.length() == 5) {
                        badMessages.add(message);
                    }

                    collected++;
                    if (collected >= messageCount) break;
                }
            }

            logger.info("=======Finished collecting messages. Good: {}, Bad: {}", goodMessages.size(), badMessages.size());

            // ACP_BLOB
            RestTemplate restTemplate = new RestTemplate();
            String acpUrl = System.getenv("ACP_STORAGE_SERVICE") + "/api/v1/blob";

            double runningTotal = 0.0;
            List<Map<String, Object>> goodToSend = new ArrayList<>();

            for (Map<String, Object> message : goodMessages) {
                runningTotal += ((Number) message.get("value")).doubleValue();
                message.put("runningTotalValue", runningTotal);

                // post to acpUrl
                try {
                    ResponseEntity<Map> response = restTemplate.postForEntity(acpUrl, message, Map.class);
                    String uuid = (String) response.getBody().get("uuid");
                    message.put("uuid", uuid);
                    goodToSend.add(message);
                } catch (Exception e) {
                    logger.error("Failed to send message to ACP", e);
                    throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "Failed to POST to ACP Storage: " + e.getMessage());
                }
            }


            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(environment.getRabbitMqHost());
            factory.setPort(environment.getRabbitMqPort());

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(writeQueueGood, false, false, false, null);
                channel.queueDeclare(writeQueueBad, false, false, false, null);

//                ObjectMapper mapper = new ObjectMapper();

                // send classified message to RabbitMQ
                for (Map<String, Object> good : goodToSend) {
                    String json = mapper.writeValueAsString(good);
                    channel.basicPublish("", writeQueueGood, null, json.getBytes(StandardCharsets.UTF_8));
                }
                for (Map<String, Object> bad : badMessages) {
                    String json = mapper.writeValueAsString(bad);
                    channel.basicPublish("", writeQueueBad, null, json.getBytes(StandardCharsets.UTF_8));
                }

                // send total end message
                double goodTotal = goodMessages.stream()
                        .mapToDouble(m -> ((Number) m.get("value")).doubleValue()).sum();
                double badTotal = badMessages.stream()
                        .mapToDouble(m -> ((Number) m.get("value")).doubleValue()).sum();

                Map<String, Object> totalGood = new LinkedHashMap<>();
                totalGood.put("uid", "s2677989");
                totalGood.put("key", "TOTAL");
                totalGood.put("comment", "good total comment");
                totalGood.put("value", goodTotal);

                Map<String, Object> totalBad = new LinkedHashMap<>();
                totalBad.put("uid", "s2677989");
                totalBad.put("key", "TOTAL");
                totalBad.put("comment", "bad total comment");
                totalBad.put("value", badTotal);

                String totalGoodJson = mapper.writeValueAsString(totalGood);
                String totalBadJson = mapper.writeValueAsString(totalBad);

                channel.basicPublish("", writeQueueGood, null, totalGoodJson.getBytes(StandardCharsets.UTF_8));
                channel.basicPublish("", writeQueueBad, null, totalBadJson.getBytes(StandardCharsets.UTF_8));
            }



            return ResponseEntity.ok("Kafka messages processed. Good: " + goodMessages.size() + ", Bad: " + badMessages.size());

        } catch (Exception e) {
            logger.error("Error during processing", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Processing failed: " + e.getMessage());
        }
    }



    @PostMapping("/transformMessages")
    public ResponseEntity<String> transformMessages(@RequestBody Map<String, Object> requestBody) {
        String readQueue = (String) requestBody.get("readQueue");
        String writeQueue = (String) requestBody.get("writeQueue");
        int messageCount = (int) requestBody.get("messageCount");

        logger.info("Transforming {} messages from {} to {}", messageCount, readQueue, writeQueue);


        int totalMessagesWritten = 0;
        int totalMessagesProcessed = 0;
        int totalRedisUpdates = 0;
        double totalValueWritten = 0.0;
        double totalAdded = 0.0;


        try (
                JedisPool jedisPool = new JedisPool(environment.getRedisHost(), environment.getRedisPort());
                Jedis jedis = jedisPool.getResource();

        ) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(environment.getRabbitMqHost());
            factory.setPort(environment.getRabbitMqPort());

            try (Connection connection = factory.newConnection();
                 Channel channel = connection.createChannel()) {

                channel.queueDeclare(readQueue, false, false, false, null);
                channel.queueDeclare(writeQueue, false, false, false, null);


                int processed = 0;
                while (processed < messageCount) {
                    GetResponse response = channel.basicGet(readQueue, true);
                    if (response == null) {
                        Thread.sleep(100);
                        continue;
                    }

                    String msgJson = new String(response.getBody(), StandardCharsets.UTF_8);
                    logger.info("Received: {}", msgJson);

                    Map<String, Object> message = mapper.readValue(msgJson, Map.class);
                    totalMessagesProcessed++;

                    String key = (String) message.get("key");
                    boolean isTombstone = !message.containsKey("version");

                    if (isTombstone) {
                        logger.info("Tombstone received for key {}", key);
                        jedis.del(key);

                        Map<String, Object> summary = new LinkedHashMap<>();
                        summary.put("totalMessagesWritten", totalMessagesWritten);
                        summary.put("totalMessagesProcessed", totalMessagesProcessed);
                        summary.put("totalRedisUpdates", totalRedisUpdates);
                        summary.put("totalValueWritten", totalValueWritten);
                        summary.put("totalAdded", totalAdded);

                        String summaryJson = mapper.writeValueAsString(summary);
                        channel.basicPublish("", writeQueue, null, summaryJson.getBytes(StandardCharsets.UTF_8));
                        logger.info("Sent tombstone summary: {}", summaryJson);
                        processed++;
                        continue;
                    }

                    int version = (Integer) message.get("version");
                    double value = ((Number) message.get("value")).doubleValue();
                    boolean shouldUpdate = false;

                    String redisVersionStr = jedis.get(key);
                    if (redisVersionStr == null) {
                        shouldUpdate = true;
                        logger.info("New key: {} → storing version {}", key, version);
                    } else {
                        int redisVersion = Integer.parseInt(redisVersionStr);
                        if (version > redisVersion) {
                            shouldUpdate = true;
                            logger.info("Updating key: {} → {} > {}", key, version, redisVersion);
                        } else {
                            logger.info("Skipping Redis update for key {} (version {} <= {})", key, version, redisVersion);
                        }
                    }

                    if (shouldUpdate) {
                        jedis.set(key, String.valueOf(version));
                        value += 10.5;
                        message.put("value", value);
                        totalRedisUpdates++;
                        totalAdded += 10.5;
                    }

                    String finalMsgJson = mapper.writeValueAsString(message);
                    channel.basicPublish("", writeQueue, null, finalMsgJson.getBytes(StandardCharsets.UTF_8));
                    totalMessagesWritten++;
                    totalValueWritten += value;
                    logger.info("Sent message: {}", finalMsgJson);

                    processed++;
                }

                logger.info("Environment ready: Redis + RabbitMQ connected");
            }
        } catch (Exception e) {
            logger.error("Error in /transformMessages", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body("Transform failed: " + e.getMessage());
        }

        return ResponseEntity.ok("Transform completed successfully");
    }


}
