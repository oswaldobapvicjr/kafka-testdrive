package net.obvj.kafkatestdrive.consumer;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
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
    
    private boolean running;

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

    public void start()
    {
        running = true;
        subscribe();
        readMessages();
    }

    private void subscribe()
    {
        try
        {
            List<String> topics = Collections.singletonList(properties.getProperty(Configuration.PROPERTY_TOPIC));

            log.log(Level.INFO, "Subscribing to topic {0}...", topics);
            consumer.subscribe(topics);
            log.info("Consumer subscription complete.");

        }
        catch (IllegalArgumentException | IllegalStateException exception)
        {
            log.log(Level.SEVERE, "Consumer subscription failed.", exception);
        }
    }
    
    /**
     * This method consume messages from Kafka topic and log them.
     */
    public void readMessages()
    {
        while (running)
        {
            log.info("Polling...");
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofSeconds(5));
            if (!consumerRecords.isEmpty())
            {
                consumerRecords.forEach(this::logRecordInfo);
                consumer.commitSync();
            }
        }
    }

    private void logRecordInfo(ConsumerRecord<String, String> record)
    {
        log.log(Level.INFO, "Message received from topic {0}, partition {1}",
                new Object[] { record.topic(), record.partition() });
        log.log(Level.INFO, "Record key: {0}", record.key());
        log.log(Level.INFO, "Record value: {0}", record.value());
    }
}
