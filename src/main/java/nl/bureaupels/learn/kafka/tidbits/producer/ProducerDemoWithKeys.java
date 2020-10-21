package nl.bureaupels.learn.kafka.tidbits.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
public class ProducerDemoWithKeys {
    private final static String TOPIC = "first_topic";

    public void go(KafkaProducer<String, String> producer) throws ExecutionException, InterruptedException {
        for (int i = 0; i < 10; i++) {
            String key = "id_" + Integer.toString(i);
            String value = "hello_world_" + Integer.toString(i);

            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, key, value);
            log.info("sending {}={}", key, value);
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("received metadata, topic {}, partition {}, offset {}, timestamp {}",
                                metadata.topic(),
                                metadata.partition(),
                                metadata.offset(),
                                metadata.timestamp());
                    } else {
                        log.error("producer error", exception);
                    }
                }
            }).get(); // this makes sending synchronous. Take care.
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        ProducerDemoWithKeys pdwk = new ProducerDemoWithKeys();

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        try {
            pdwk.go(producer);
        } catch (Exception x) {
            log.error("Exception occurred: ", x);
        }
        producer.close();
    }
}
