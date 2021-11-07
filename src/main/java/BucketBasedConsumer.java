import io.github.bucket4j.Bandwidth;
import io.github.bucket4j.Bucket;
import io.github.bucket4j.Bucket4j;
import io.github.bucket4j.Refill;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class BucketBasedConsumer {

    Logger log = LoggerFactory.getLogger(BucketBasedConsumer.class);

    KafkaConsumer<String, String> consumer;
    Bucket bucket;

    public BucketBasedConsumer(KafkaConsumer<String, String> consumer) {
        this.consumer = consumer;
        bucket = createNewBucket();
    }

    public ConsumerRecords<String, String> consume() {
        ConsumerRecords<String, String> records = null;

        log.info("Amount of tokens available: " + bucket.getAvailableTokens());

        records = this.consumer.poll(Duration.ofMillis(1000));

        log.info("Amount of records returned by poll " + records.count());

        // Consume x tokens from the bucket. If the bucket doesn't have this amount of available tokens
        // this method will block until the refill adds the amount of necessary tokens
        if(records.count() > 0) {
            if (bucket.tryConsume(records.count())) {
                log.info("Successfully consumed " + records.count() + " records. Available tokens now: " + bucket.getAvailableTokens());
                return records;
            } else {
                try {
                    bucket.asScheduler().consume(records.count());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        //consumer.close(); //this gives an exception, this consumer has already been closed
        return records;
    }

    public Bucket createNewBucket() {
        long capacity = 50;
        //specifies the speed of token regeneration (leva 5 segundos para enxer ate 50)
        Refill refill = Refill.greedy(10, Duration.ofSeconds(1));
        //Bandwidth defines the limit of the bucket. We use it to configure the capacity and the rate of refill
        Bandwidth limit = Bandwidth.classic(capacity, refill);
        return Bucket4j.builder().addLimit(limit).build();
    }
}
