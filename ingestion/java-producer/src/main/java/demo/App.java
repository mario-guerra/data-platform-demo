package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Kafka producer that generates synthetic order events
 */
public class App {
    private static final String TOPIC = "orders";
    private static final String[] PRODUCTS = {
        "laptop", "smartphone", "tablet", "headphones", "keyboard", 
        "mouse", "monitor", "webcam", "speakers", "printer"
    };
    private static final String[] CURRENCIES = {"USD", "EUR", "GBP", "JPY", "CAD"};
    private static final Random RANDOM = new Random();
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) {
        String bootstrapServers = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            System.out.println("Starting Kafka producer, sending to topic: " + TOPIC);
            System.out.println("Bootstrap servers: " + bootstrapServers);
            
            for (int i = 0; i < 1000; i++) {
                ObjectNode order = generateOrder();
                String key = order.get("order_id").asText();
                String value = MAPPER.writeValueAsString(order);
                
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        System.err.println("Error sending record: " + exception.getMessage());
                    } else {
                        System.out.printf("Sent order %s to partition %d offset %d%n", 
                            key, metadata.partition(), metadata.offset());
                    }
                });
                
                // Send orders at different rates throughout the day
                Thread.sleep(getRandomDelay());
            }
            
            producer.flush();
            System.out.println("Finished sending orders");
            
        } catch (Exception e) {
            System.err.println("Error in producer: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static ObjectNode generateOrder() {
        ObjectNode order = MAPPER.createObjectNode();
        order.put("order_id", UUID.randomUUID().toString());
        order.put("customer_id", "customer_" + ThreadLocalRandom.current().nextInt(1, 1001));
        order.put("product", PRODUCTS[RANDOM.nextInt(PRODUCTS.length)]);
        order.put("quantity", ThreadLocalRandom.current().nextInt(1, 6));
        order.put("price", Math.round(ThreadLocalRandom.current().nextDouble(10.0, 1000.0) * 100.0) / 100.0);
        order.put("currency", CURRENCIES[RANDOM.nextInt(CURRENCIES.length)]);
        order.put("timestamp", Instant.now().toString());
        order.put("region", getRandomRegion());
        return order;
    }
    
    private static String getRandomRegion() {
        String[] regions = {"us-east", "us-west", "eu-central", "asia-pacific"};
        return regions[RANDOM.nextInt(regions.length)];
    }
    
    private static long getRandomDelay() {
        // Simulate varying load throughout the day
        int hour = java.time.LocalTime.now().getHour();
        if (hour >= 9 && hour <= 17) {
            // Business hours - higher frequency
            return ThreadLocalRandom.current().nextLong(100, 500);
        } else {
            // Off hours - lower frequency
            return ThreadLocalRandom.current().nextLong(1000, 3000);
        }
    }
}