import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerMain {

    private static final Logger log = LoggerFactory.getLogger(ConsumerMain.class);

    public static void main(String[] args) {
        log.info("I'm a Kafka Consumer");

        // create Producer properties
        log.info("Reading Kafka Config file");
        Properties properties = new Properties();
        String kafkaTopic;
        try {
            properties.load(ConsumerMain.class.getResourceAsStream("kafka.conf"));

            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getProperty("bootstrapServers"));
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty("consumerGroupId"));
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getProperty("autoOffsetResetConfig"));

            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            kafkaTopic = properties.getProperty("kafkaTopic");
        } catch (IOException e) {
            log.error(String.valueOf(e));
            throw new RuntimeException(e);
        }

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(kafkaTopic));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                log.info("Key: " + record.key() + ", Value: " + record.value());
                log.info("Partition: " + record.partition() + ", Offset:" + record.offset());
            }
        }

    }
}
