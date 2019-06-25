package net.obvj.kafkatestdrive.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import net.obvj.kafkatestdrive.config.Configuration;

/**
 * This class is responsible to retrieve messages from a Kafka Topic configured via
 * properties file.
 */
public class KafkaConsumerService
{
    private final Logger log = Logger.getLogger(KafkaConsumerService.class.getName());

    private Consumer<String, String> consumer;
    private Properties properties;

    /**
     * KafkaProducerService constructor
     *
     * @param consumer
     * @param properties
     */
    public KafkaConsumerService(Consumer<String, String> consumer, Properties properties)
    {
        this.consumer = consumer;
        this.properties = properties;
    }

    public void consumeMessageFromKafkaTopic()
    {
        subscribe();
        readMessages();
    }

    /**
     * This method consume messages from Kafka topic and log them.
     */
    public void readMessages()
    {
        while (true)
        {
            try
            {
                final ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(1));
                if (consumerRecords.isEmpty())
                {
                    TimeUnit.SECONDS.sleep(5);
                    continue;
                }

                for (ConsumerRecord<String, String> record : consumerRecords)
                {
                    logRecordInfo(record);
                }
                consumer.commitSync();

            }
            catch (InterruptedException exception)
            {
                log.log(Level.WARNING, "Interrupted while sleeping. Restoring interrupted state...", exception);
            }
        }
    }

    private void subscribe()
    {
        try
        {
            List<String> topics = Collections.singletonList(properties.getProperty(Configuration.PROPERTY_TOPIC));
            log.info("Kafka consumer started! topic = " + topics.toString());

            log.info("Consumer subscribe start.");
            consumer.subscribe(topics);
            log.info("Consumer subscribe done.");

        }
        catch (IllegalArgumentException | IllegalStateException exception)
        {
            log.log(Level.SEVERE, "Consumer subscribe failed.", exception);
        }
    }

    private void logRecordInfo(ConsumerRecord<String, String> record)
    {
        log.info("Message received from topic: " + record.topic() + " partition: " + record.partition());
        log.info("Record key: " + record.key());
        log.info("Record value: " + record.value());
    }
}
