package net.obvj.kafkatestdrive.consumer;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import net.obvj.kafkatestdrive.config.Configuration;

public class KafkaMessageConsumer
{
    private Properties properties;

    public KafkaMessageConsumer(Properties properties)
    {
        this.properties = properties;
    }

    /**
     * Creates a new Kafka API Consumer
     * 
     * @return KafkaConsumer
     */
    public Consumer<String, String> createConsumer()
    {
        Properties lProperties = new Properties();
        lProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_BOOTSTRAP_SERVERS_CONFIG));
        lProperties.put(ConsumerConfig.CLIENT_ID_CONFIG,
                properties.getProperty(Configuration.PROPERTY_CLIENT_ID_CONFIG));
        lProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_KEY_DESERIALIZER_CLASS_CONFIG));
        lProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_VALUE_DESERIALIZER_CLASS_CONFIG));
        lProperties.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getProperty(Configuration.PROPERTY_GROUP_ID_CONFIG));
        lProperties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,
                Integer.valueOf(properties.getProperty(Configuration.PROPERTY_MAX_POLL_RECORDS_CONFIG)));
        lProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
                Boolean.valueOf(properties.getProperty(Configuration.PROPERTY_ENABLE_AUTO_COMMIT_CONFIG)));
        return new KafkaConsumer<>(lProperties);
    }
}
