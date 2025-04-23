package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.client.RestTemplate;
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
                ResponseEntity<Map> response = restTemplate.postForEntity(acpUrl, message, Map.class);
                String uuid = (String) response.getBody().get("uuid");

                message.put("uuid", uuid);
                goodToSend.add(message);
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
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Processing failed: " + e.getMessage()); // todo status code
        }
    }

}
