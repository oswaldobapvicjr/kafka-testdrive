package net.obvj.kafkatestdrive.producer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Properties;

import net.obvj.kafkatestdrive.config.Configuration;
import net.obvj.kafkatestdrive.config.Configuration.Mode;

public class ProducerMain
{
    public static void main(String[] args) throws IOException
    {
        Configuration configuration = new Configuration(Mode.PRODUCER);

        Properties properties = configuration.loadPropertiesFile();

        KafkaProducerService producer = new KafkaProducerService(properties);
        
        Path jsonFilesPath = new File(configuration.getProducerInputPath()).toPath();

        // First read all files from the source directory
        producer.consumeJsonFromDirectoryPath(jsonFilesPath);
        
        // Then start watching new files
        producer.watchJsonDirectoryPath(jsonFilesPath);
    }
}
