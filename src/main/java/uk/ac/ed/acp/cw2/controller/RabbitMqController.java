package uk.ac.ed.acp.cw2.controller;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import uk.ac.ed.acp.cw2.data.RuntimeEnvironment;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RabbitMqController is a REST controller that provides endpoints for sending and receiving stock symbols
 * through RabbitMQ. This class interacts with a RabbitMQ environment which is configured dynamically during runtime.
 */
@RestController()
@RequestMapping("/rabbitMq")
public class RabbitMqController {

    private static final Logger logger = LoggerFactory.getLogger(RabbitMqController.class);
    private final RuntimeEnvironment environment;
    private final String[] stockSymbols = "AAPL,MSFT,GOOG,AMZN,TSLA,JPMC,CATP,UNIL,LLOY".split(",");

    private ConnectionFactory factory = null;

    public RabbitMqController(RuntimeEnvironment environment) {
        this.environment = environment;
        factory = new ConnectionFactory();
        factory.setHost(environment.getRabbitMqHost());
        factory.setPort(environment.getRabbitMqPort());
    }


    public final String StockSymbolsConfig = "stock.symbols";

    @PutMapping("/{queueName}/{messageCount}")
    public ResponseEntity<String> sendMessagesToQueue(@PathVariable String queueName, @PathVariable int messageCount) {
        logger.info("Writing {} messages in queue {}", messageCount, queueName);
        String studentId = "s2677989";

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < messageCount; i++) {
                String json = String.format("{\"uid\":\"%s\", \"counter\":%d}", studentId, i);

//                final String symbol = stockSymbols[new Random().nextInt(stockSymbols.length)];
//                final String value = String.valueOf(i);
//
//                String message = String.format("%s:%s", symbol, value);

                channel.basicPublish("", queueName, null, json.getBytes());
                System.out.println(" [x] Sent message: " + json + " to queue: " + queueName);
            }

            logger.info("{} JSON message(s) sent to RabbitMQ queue {}\n", messageCount, queueName);
            return ResponseEntity.ok(messageCount + " messages sent to " + queueName);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "RabbitMQ send failed");
        }
    }

    @GetMapping("/{queueName}/{timeoutInMsec}")
    public ResponseEntity<List<String>> receiveMessages(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
        logger.info(String.format("Reading messages from queue %s", queueName));
        List<String> result = new ArrayList<>();

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            // 注册回调函数：每收到一条消息就添加到 result 列表中。
            DeliverCallback deliverCallback = (consumerTag, delivery) -> {
                String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
                System.out.printf("[%s]:%s -> %s", queueName, delivery.getEnvelope().getRoutingKey(), message);
                result.add(message);
            };

            System.out.println("start consuming events - to stop press CTRL+C");
            // Consume with Auto-ACK.  消费队列数据（自动确认 ACK），等待一段时间，然后自动停止。
            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {});
            Thread.sleep(timeoutInMsec);

            System.out.printf("done consuming events. %d record(s) received\n", result.size());
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Queue read failed");
        }
    }


    @PostMapping("/testTransformMessages")
    public void sendTransformTestData(@RequestParam String queueName) throws Exception {

        try (Connection conn = factory.newConnection(); Channel channel = conn.createChannel()) {
            channel.queueDeclare(queueName, false, false, false, null);

            ObjectMapper mapper = new ObjectMapper();

            List<Map<String, Object>> testData = List.of(
                    Map.of("key", "ABC", "version", 1, "value", 100.0),
                    Map.of("key", "ABC", "version", 1, "value", 200.0),
                    Map.of("key", "ABC", "version", 3, "value", 400.0),
                    Map.of("key", "ABC", "version", 2, "value", 200.0),
                    Map.of("key", "ABC"),
                    Map.of("key", "ABC", "version", 2, "value", 200.0)
            );

            for (Map<String, Object> msg : testData) {
                String json = mapper.writeValueAsString(msg);
                channel.basicPublish("", queueName, null, json.getBytes(StandardCharsets.UTF_8));
                logger.info("Sent test message: {}", json);
            }
        }
    }

}
