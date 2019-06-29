package net.obvj.kafkatestdrive.consumer;

import java.util.Properties;

import net.obvj.kafkatestdrive.config.Configuration;
import net.obvj.kafkatestdrive.config.Configuration.Mode;

public class ConsumerMain
{
    public static void main(String[] args)
    {
        Configuration configuration = new Configuration(Mode.CONSUMER);

        Properties properties = configuration.loadPropertiesFile();

        KafkaConsumerService consumer = new KafkaConsumerService(properties);
        consumer.start();
    }
}
