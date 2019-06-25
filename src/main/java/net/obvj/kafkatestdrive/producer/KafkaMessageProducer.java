package net.obvj.kafkatestdrive.producer;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import net.obvj.kafkatestdrive.config.Configuration;

public class KafkaMessageProducer
{
    private Properties properties;

    public KafkaMessageProducer(Properties properties)
    {
        this.properties = properties;
    }

    /**
     * Creates a new Kafka API Producer
     *
     * @return KafkaProducer
     */
    public Producer<String, String> createProducer()
    {
        Properties lProperties = new Properties();
        lProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_BOOTSTRAP_SERVERS_CONFIG));
        lProperties.put(ProducerConfig.CLIENT_ID_CONFIG,
                properties.getProperty(Configuration.PROPERTY_CLIENT_ID_CONFIG));
        lProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_KEY_SERIALIZER_CLASS_CONFIG));
        lProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                properties.getProperty(Configuration.PROPERTY_VALUE_SERIALIZER_CLASS_CONFIG));
        return new KafkaProducer<>(lProperties);
    }
}
