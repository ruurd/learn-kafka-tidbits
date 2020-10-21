package nl.bureaupels.learn.kafka.udemy.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

@Slf4j
public class ConsumerDemoWithThreads {
    private final static String TOPIC = "first_topic";
    private final static String GROUP = "first_topic_reader";
    private final static int NUMCONSUMERS = 3;

    public static void main(String[] args) {
        ConsumerDemoWithThreads app = new ConsumerDemoWithThreads();
        app.go();
    }

    public void go() {
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try {
            CountDownLatch latch = new CountDownLatch(NUMCONSUMERS);
            for (int i = 0; i < NUMCONSUMERS; i++) {
                Runnable consumerRunnable = new ConsumerRunnable(latch, properties, TOPIC);
                Thread consumerThread = new Thread(consumerRunnable);
                consumerThread.start();

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    ((ConsumerRunnable) consumerRunnable).shutDown();
                    try {
                        latch.await();
                    } catch (InterruptedException ix) {
                        log.error("interrupted await", ix);
                    }
                }));
            }
            latch.await();
        } catch (InterruptedException ix) {
            log.error("interrupted", ix);
        }
        log.info("shutting down");
    }

    public class ConsumerRunnable implements Runnable {
        private CountDownLatch latch;
        private String topic;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(CountDownLatch latch, Properties properties, String topic) {
            this.latch = latch;
            this.topic = topic;
            consumer = new KafkaConsumer<>(properties);
        }

        @Override
        public void run() {
            try {
                consumer.subscribe(Collections.singleton(topic));
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord record : records) {
                        log.info("partition {} key {} value {} at offset {}", record.partition(), record.key(), record.value(), record.offset());
                    }
                }
            } catch (WakeupException wx) {
                log.info("consumer wake up");
            } finally {
                consumer.close();
                latch.countDown();
            }
            log.info("consumer stop");
        }

        public void shutDown() {
            consumer.wakeup();
        }
    }
}
