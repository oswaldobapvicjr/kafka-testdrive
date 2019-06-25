package net.obvj.kafkatestdrive.main;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;

import net.obvj.kafkatestdrive.config.Configuration;
import net.obvj.kafkatestdrive.config.Configuration.Mode;
import net.obvj.kafkatestdrive.consumer.KafkaConsumerService;
import net.obvj.kafkatestdrive.consumer.KafkaMessageConsumer;

public class ConsumerMain
{
    public static void main(String[] args)
    {
        Configuration kafkaMessageSimulatorConfiguration = new Configuration(Mode.CONSUMER);

        Properties properties = kafkaMessageSimulatorConfiguration.readFileProperties();
        KafkaMessageConsumer kafkaMessageConsumer = new KafkaMessageConsumer(properties);

        try (Consumer<String, String> consumer = kafkaMessageConsumer.createConsumer())
        {
            KafkaConsumerService kafkaConsumerService = new KafkaConsumerService(consumer, properties);
            kafkaConsumerService.consumeMessageFromKafkaTopic();
        }
    }
}
