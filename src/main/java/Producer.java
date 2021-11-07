import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Producer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger log = LoggerFactory.getLogger(Producer.class);

        String bootstrapServers = "127.0.0.1:9092";

        // 1. CREATE THE PRODUCER PROPERTIES

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 2. CREATE THE PRODUCER

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // 3. CREATE A PRODUCER RECORD
        for (int i=350; i<450; i++) {

            String topic = "test-throttle";
            String value = "hello world " + i;
            String key = "id_" + i;

            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);

            log.info("Key: " + key); // log the key

            // 4. SEND DATA
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // runs every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        // the record was successfully sent
                        log.info("Received new metadata. \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());
                    } else {
                        log.error("Error while producing", e);
                    }
                }
            }).get(); // block the .send() to make it synchronous - don't do this in production!
        }

        // Wait for the data to be produced
        producer.flush(); //flush data
        producer.close(); //flush and close producer

    }
}
