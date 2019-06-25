package net.obvj.kafkatestdrive.main;

import java.io.File;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;

import net.obvj.kafkatestdrive.config.Configuration;
import net.obvj.kafkatestdrive.config.Configuration.Mode;
import net.obvj.kafkatestdrive.producer.KafkaMessageProducer;
import net.obvj.kafkatestdrive.producer.KafkaProducerService;

public class ProducerMain
{
    public static void main(String[] args)
    {
        Configuration kafkaMessageSimulatorPropertiesReader = new Configuration(Mode.PRODUCER);

        Properties propertyFile = kafkaMessageSimulatorPropertiesReader.readFileProperties();
        KafkaMessageProducer kafkaMessageProducer = new KafkaMessageProducer(propertyFile);

        try (Producer<String, String> producer = kafkaMessageProducer.createProducer())
        {
            KafkaProducerService kafkaProducerService = new KafkaProducerService(producer, propertyFile);
            Path jsonFilesPath = new File(kafkaMessageSimulatorPropertiesReader.getJsonFilesPath()).toPath();
            kafkaProducerService.consumeJsonFromDirectoryPath(jsonFilesPath);
            kafkaProducerService.watchJsonDirectoryPath(jsonFilesPath);
        }
    }
}
