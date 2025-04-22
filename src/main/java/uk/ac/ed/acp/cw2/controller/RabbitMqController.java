package uk.ac.ed.acp.cw2.controller;


import com.rabbitmq.client.DeliverCallback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;
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
@RequestMapping("/api/v1/rabbitmq")
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
    public void sendMessagesToQueue(@PathVariable String queueName, @PathVariable int messageCount) {
        logger.info("Writing {} messages in queue {}", messageCount, queueName);
        String studentId = "s2677989";

        try (Connection connection = factory.newConnection();
             Channel channel = connection.createChannel()) {

            channel.queueDeclare(queueName, false, false, false, null);

            for (int i = 0; i < messageCount; i++) {
                String json = String.format("{\"uuid\":\"%s\", \"counter\":%d}", studentId, i);

//                final String symbol = stockSymbols[new Random().nextInt(stockSymbols.length)];
//                final String value = String.valueOf(i);
//
//                String message = String.format("%s:%s", symbol, value);

                channel.basicPublish("", queueName, null, json.getBytes());
                System.out.println(" [x] Sent message: " + json + " to queue: " + queueName);
            }

            logger.info("{} JSON message(s) sent to RabbitMQ queue {}\n", messageCount, queueName);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @GetMapping("/{queueName}/{timeoutInMsec}")
    public List<String> receiveMessages(@PathVariable String queueName, @PathVariable int timeoutInMsec) {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}
