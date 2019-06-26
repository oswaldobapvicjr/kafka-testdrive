package net.obvj.kafkatestdrive.config;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Configuration
{
    public enum Mode
    {
        PRODUCER(PRODUCER_PROPERTIES, defaultProducerProperties()),
        CONSUMER(CONSUMER_PROPERTIES, defaultConsumerProperties());

        final String fileName;
        final Properties defaultProperties;

        private Mode(String fileName, Properties defaultProperties)
        {
            this.fileName = fileName;
            this.defaultProperties = defaultProperties;
        }

        public String getFileName()
        {
            return fileName;
        }

        public Properties getDefaultProperties()
        {
            return defaultProperties;
        }

    }

    /*
     * Resources directories
     */
    public static final String PRODUCER_INPUT_DIR = "producer-input";
    public static final String RESOURCES_DIR = "resources/";

    /*
     * Configuration files
     */
    public static final String PRODUCER_PROPERTIES = "producer.properties";
    public static final String CONSUMER_PROPERTIES = "consumer.properties";

    public static final String PROPERTY_BOOTSTRAP_SERVERS_CONFIG = "BOOTSTRAP_SERVERS_CONFIG";
    public static final String PROPERTY_TOPIC = "TOPIC";
    public static final String PROPERTY_CLIENT_ID_CONFIG = "CLIENT_ID_CONFIG";
    public static final String PROPERTY_KEY_SERIALIZER_CLASS_CONFIG = "KEY_SERIALIZER_CLASS_CONFIG";
    public static final String PROPERTY_VALUE_SERIALIZER_CLASS_CONFIG = "VALUE_SERIALIZER_CLASS_CONFIG";
    public static final String PROPERTY_GROUP_ID_CONFIG = "GROUP_ID_CONFIG";
    public static final String PROPERTY_MAX_POLL_RECORDS_CONFIG = "MAX_POLL_RECORDS_CONFIG";
    public static final String PROPERTY_KEY_DESERIALIZER_CLASS_CONFIG = "KEY_DESERIALIZER_CLASS_CONFIG";
    public static final String PROPERTY_VALUE_DESERIALIZER_CLASS_CONFIG = "VALUE_DESERIALIZER_CLASS_CONFIG";
    public static final String PROPERTY_ENABLE_AUTO_COMMIT_CONFIG = "ENABLE_AUTO_COMMIT_CONFIG";
    public static final String PROPERTY_MAX_MESSAGE_FILES_BACKUP_INDEX = "MAX_MESSAGE_FILES_BACKUP_INDEX";
    public static final String PROPERTY_OUTPUT_MESSAGE_JSON_PATH = "OUTPUT_MESSAGE_JSON_PATH";

    /**
     * Pattern to match environment variables in format ${VAR_NAME}
     */
    private static final Pattern PATTERN_ENVIRONMENT_VARIABLE = Pattern.compile("\\$\\{?([\\w]+)\\}?");

    /*
     * Default properties
     */
    private static final String BOOTSTRAP_SERVERS_CONFIG = "localhost:9092";
    private static final String KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";
    private static final String VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.ByteArraySerializer";
    private static final String KEY_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String VALUE_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.ByteArrayDeserializer";
    private static final String CLIENT_ID_CONFIG = "defaultClient";
    private static final String PRODUCER_TOPIC = "testTopic1";
    private static final String CONSUMER_TOPIC = "testTopic1";
    private static final String GROUP_ID_CONFIG = "defaultGroup";
    private static final String MAX_POLL_RECORDS_CONFIG = "1";
    private static final String ENABLE_AUTO_COMMIT_CONFIG = "false";

    private final Logger log = Logger.getLogger(Configuration.class.getName());

    private final Mode mode;

    public Configuration(Mode mode)
    {
        this.mode = mode;
    }

    private static Properties defaultProducerProperties()
    {
        Properties properties = new Properties();
        properties.put(PROPERTY_BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(PROPERTY_CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        properties.put(PROPERTY_KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER_CLASS_CONFIG);
        properties.put(PROPERTY_VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER_CLASS_CONFIG);
        properties.put(PROPERTY_TOPIC, PRODUCER_TOPIC);
        return properties;
    }

    private static Properties defaultConsumerProperties()
    {
        Properties properties = new Properties();
        properties.put(PROPERTY_BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS_CONFIG);
        properties.put(PROPERTY_CLIENT_ID_CONFIG, CLIENT_ID_CONFIG);
        properties.put(PROPERTY_KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER_CLASS_CONFIG);
        properties.put(PROPERTY_VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER_CLASS_CONFIG);
        properties.put(PROPERTY_GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        properties.put(PROPERTY_TOPIC, CONSUMER_TOPIC);
        properties.put(PROPERTY_MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS_CONFIG);
        properties.put(PROPERTY_ENABLE_AUTO_COMMIT_CONFIG, ENABLE_AUTO_COMMIT_CONFIG);
        return properties;
    }

    /**
     * Reads kafka message simulator properties from file
     *
     * @return propertiesMap
     */
    public Properties readFileProperties()
    {
        Properties properties;
        File baseDirectoryFile = getPropertiesFile();
        try (InputStream input = new FileInputStream(baseDirectoryFile))
        {
            properties = fillProperties(input, mode.getDefaultProperties());
        }
        catch (IOException exception)
        {
            log.log(Level.WARNING, "Unable to read properties file. Starting with default values...", exception);

            properties = mode.getDefaultProperties();
        }

        for (Entry<Object, Object> returnedProperty : properties.entrySet())
        {
            log.log(Level.INFO, "{0}: {1}", new Object[] { returnedProperty.getKey(), returnedProperty.getValue() });
        }
        return properties;
    }

    /**
     * @return the path where input-files are read from
     */
    public String getProducerInputPath()
    {
        return "./" + RESOURCES_DIR + "/" + PRODUCER_INPUT_DIR;
    }

    private Properties fillProperties(InputStream pInput, Properties pDefaultProperties) throws IOException
    {
        Properties properties = new Properties();
        properties.load(pInput);
        for (Object propertyField : pDefaultProperties.keySet())
        {
            String fileProperty = properties.getProperty((String) propertyField);
            if (fileProperty != null && !fileProperty.isEmpty())
            {
                Matcher matcher = PATTERN_ENVIRONMENT_VARIABLE.matcher(fileProperty);
                if (matcher.matches())
                {
                    String environmentVariableName = matcher.group(1);
                    fileProperty = System.getenv(environmentVariableName);
                    if (fileProperty == null || fileProperty.isEmpty())
                    {
                        fileProperty = pDefaultProperties.getProperty((String) propertyField);
                    }
                }
                pDefaultProperties.put(propertyField, fileProperty);
            }
        }
        return pDefaultProperties;
    }

    private File getPropertiesFile()
    {
        File configFile = null;
        String baseDir = "./";
        configFile = new File(baseDir, RESOURCES_DIR + mode.fileName);
        log.info("settings=" + configFile.getAbsolutePath());
        return configFile;
    }
}
