package uk.ac.ed.acp.cw2.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;

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

                    ObjectMapper mapper = new ObjectMapper();
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

            // TODO: 下一步写入 ACP Storage + RabbitMQ
            return ResponseEntity.ok("Kafka messages processed. Good: " + goodMessages.size() + ", Bad: " + badMessages.size());

        } catch (Exception e) {
            logger.error("Error during processing", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Processing failed: " + e.getMessage()); // todo状态码
        }
    }

}
