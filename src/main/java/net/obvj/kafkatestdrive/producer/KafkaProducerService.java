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
 * This class is responsible for sending messages to a Kafka topic
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
    public void consumeJsonFromPath(Path path) throws IOException
    {
        File[] fileList = getFileList(path);
        for (File file : fileList)
        {
            log.info("Reading file: " + file.getName());
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
            log.log(Level.INFO, "Watching path: {0}", path);
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

                        log.log(Level.INFO, "New file found: {0}", newPath);
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
                log.log(Level.WARNING, "Interrupted while waiting for files.", iex);
                Thread.currentThread().interrupt();
            }
        }
    }

    private void runProducer(Path path)
    {
        try
        {
            TimeUnit.SECONDS.sleep(1);
        }
        catch (InterruptedException iex)
        {
            log.log(Level.WARNING, "Interrupted while waiting", iex);
            Thread.currentThread().interrupt();
        }

        File jsonFile = path.toFile();
        if (!jsonFile.exists())
        {
            log.warning("Input file not found.");
            return;
        }
        try
        {
            String jsonString = readFile(path.toAbsolutePath().toString());
            JSONObject jsonObject = new JSONObject(jsonString);
            sendMessage(jsonObject);
        }
        catch (Exception exception)
        {
            log.log(Level.WARNING, "Error reading file", exception);
        }
    }

    private void sendMessage(JSONObject jsonObject)
    {
        String topicName = properties.getProperty(Configuration.TOPIC_NAME);

        log.log(Level.FINEST, "JSON: {0}", jsonObject);
        try
        {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, jsonObject.toString());
            RecordMetadata metadata = producer.send(record).get();
            log.log(Level.INFO, "Sent record [key = {0}, value = {1}, meta (patition = {2}, offset = {3})]",
                    new Object[] { record.key(), record.value(), metadata.partition(), metadata.offset() });
        }
        catch (InterruptedException e)
        {
            log.log(Level.WARNING, "Interrupted while sending message", e);
            Thread.currentThread().interrupt();
        }
        catch (ExecutionException e)
        {
            log.log(Level.WARNING, "Error sending message to kafka topic", e);
        }
    }

    private void deleteFile(Path path) throws IOException
    {
        Files.delete(path);
        log.log(Level.INFO, "File deleted: {0}", path);
    }

    private File[] getFileList(Path path)
    {
        File directory = new File(path.toString());
        return directory.listFiles((dir, name) -> name.endsWith(JSON));
    }

    private String readFile(String filename)
    {
        try (BufferedReader br = new BufferedReader(new FileReader(filename)))
        {
            StringBuilder sb = new StringBuilder();
            String line = br.readLine();
            while (line != null)
            {
                sb.append(line);
                line = br.readLine();
            }
            return sb.toString();
        }
        catch (Exception e)
        {
            log.log(Level.WARNING, "Error reading file: " + filename, e);
            return null;
        }
    }
}
