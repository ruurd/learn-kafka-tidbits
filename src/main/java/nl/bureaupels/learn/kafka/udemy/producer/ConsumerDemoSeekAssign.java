package nl.bureaupels.learn.kafka.udemy.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class ConsumerDemoSeekAssign {
    private final static String TOPIC = "first_topic";

    public void go(KafkaConsumer consumer) {
        TopicPartition partition = new TopicPartition(TOPIC, 0);
        long offset = 15L;
        consumer.assign(Collections.singletonList(partition));
        consumer.seek(partition, offset);

        int numToRead = 5;
        int numRead = 0;
        boolean keepReading = true;
        while(keepReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord record: records) {
                numRead += 1;
                log.info("partition {} key {} value {} at offset {}", record.partition(), record.key(), record.value(), record.offset());
                if (numRead >= numToRead) {
                    keepReading = false;
                    break;
                }
            }
        }
    }

    public static void main(String[] args) {
        ConsumerDemoSeekAssign app = new ConsumerDemoSeekAssign();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        app.go(consumer);
        consumer.close();
    }
}
