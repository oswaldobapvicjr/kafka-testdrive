package net.obvj.kafkatestdrive.producer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.*;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.codehaus.jettison.json.JSONObject;

import net.obvj.kafkatestdrive.config.Configuration;

/**
 * This class is responsible for sending messages to a kafka topic configured via
 * properties files
 */
public class KafkaProducerService
{
    private static final String JSON = ".json";

    private final Logger log = Logger.getLogger(KafkaProducerService.class.getName());

    private Producer<String, String> producer;
    private Properties properties;

    /**
     * KafkaProducerService constructor
     *
     * @param producer
     * @param propertyFile
     */
    public KafkaProducerService(Properties propertyFile)
    {
        this.properties = propertyFile;
        this.producer = new KafkaProducer<>(properties);
    }

    /**
     * This method consumes .json files from specific path
     *
     * @param path
     * @throws IOException 
     */
    public void consumeJsonFromDirectoryPath(Path path) throws IOException
    {
        File[] fileList = getFileList(path);
        for (File file : fileList)
        {
            log.info("Consuming : " + file.getName());
            runProducer(file.toPath());
            deleteFile(file.toPath());
        }
    }

    /**
     * This method watches directory path waiting for new json files.
     *
     * @param path
     */
    public void watchJsonDirectoryPath(Path path)
    {
        try (FileSystem fs = path.getFileSystem(); WatchService service = fs.newWatchService())
        {
            path.register(service, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
            log.info("Watching path: " + path);
            watch(service);
        }
        catch (IOException ioe)
        {
            log.log(Level.WARNING, "Error when getting new watch service.", ioe);
        }
    }

    private void watch(WatchService service) throws IOException
    {
        while (true)
        {
            try
            {
                WatchKey key = service.take();
                for (WatchEvent<?> watchEvent : key.pollEvents())
                {
                    if (StandardWatchEventKinds.ENTRY_CREATE == watchEvent.kind())
                    {
                        Path newPath = ((Path) key.watchable()).resolve(((WatchEvent<Path>) watchEvent).context());

                        log.info("New file found: " + newPath);
                        runProducer(newPath);
                        deleteFile(newPath);
                    }
                }
                if (!key.reset())
                {
                    break;
                }
            }
            catch (InterruptedException iex)
            {
                log.log(Level.WARNING, "Interrupted", iex);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runProducer(Path newPath)
    {
        try
        {
            TimeUnit.SECONDS.sleep(1);
        }
        catch (InterruptedException iex)
        {
            log.log(Level.WARNING, "Interrupted", iex);
            Thread.currentThread().interrupt();
        }

        File jsonFile = newPath.toFile();
        if (!jsonFile.exists())
        {
            log.warning("Json file not found! Do nothing.");
            return;
        }
        try
        {
            String jsonString = readFile(newPath.toAbsolutePath().toString());
            JSONObject jsonObject = new JSONObject(jsonString);
            sendMessage(jsonObject);
        }
        catch (Exception exception)
        {
            log.log(Level.WARNING, "Error when reading .json file.", exception);
        }
    }

    private void sendMessage(JSONObject jsonObject)
    {
        String topicName = properties.getProperty(Configuration.TOPIC_NAME);

        log.finest(jsonObject.toString());
        try
        {
            ProducerRecord<String, String> record;
            record = new ProducerRecord<>(topicName, jsonObject.toString());
            RecordMetadata metadata = producer.send(record).get();
            log.info("sent record(key = " + record.key() + " value = " + record.value() + "] " + "meta(partition = "
                    + Integer.valueOf(metadata.partition()) + " " + "offset = " + Long.valueOf(metadata.offset())
                    + ")");
        }
        catch (InterruptedException | ExecutionException e)
        {
            log.log(Level.WARNING, "Error when sending a message to kafka topic. The message will not be sent.", e);
        }
    }

    private void deleteFile(Path filePath) throws IOException
    {
        Files.delete(filePath);
        log.info("File deleted: " + filePath);
    }

    private File[] getFileList(Path ath)
    {
        File directory = new File(ath.toString());
        return directory.listFiles((dir, name) -> name.endsWith(JSON));
    }

    private String readFile(String filename)
    {
        String result = "";
        try (BufferedReader br = new BufferedReader(new FileReader(filename)))
        {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null)
            {
                sb.append(line);
                line = br.readLine();
            }
            result = sb.toString();
        }
        catch (Exception e)
        {
            log.log(Level.WARNING, "Error reading file name: " + filename, e);
        }
        return result;
    }
}
