package edu.bigdata.twitter2kafka;

import org.apache.commons.codec.binary.Base64;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import java.io.*;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class TwitterProducer implements Runnable {
    private static final ConfigSt CONFIG = ConfigSt.getInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private static final String CONSUMER_KEY = CONFIG.getConfig("consumer_key"),
            CONSUMER_SECRET = CONFIG.getConfig("consumer_secret"),
            ENDPOINT_AUTHENTICATION = "https://api.twitter.com/oauth2/token",
            ENDPOINT_TWEET_SEARCH = "https://api.twitter.com/1.1/search/tweets.json",
            KAFKA_SERVER = CONFIG.getConfig("kafka_server"),
            KAFKA_TOPIC = CONFIG.getConfig("kafka_topic"),
            QUERY_FILTER = CONFIG.getConfig("query_filter");

    private TwitterProducer() {
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    private KafkaProducer<String, String> createKafkaProducer() {
        // Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // High Throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32768)); // 32 KB

        // Create Producer
        return new KafkaProducer<>(properties);
    }

    private JSONArray getTweets(String bearerToken, long lastId) throws IOException {
        JSONArray tweetArr = null;
        HttpsURLConnection connection = null;
        try {
            URL url = new URL(ENDPOINT_TWEET_SEARCH + "?q=" + URLEncoder.encode(QUERY_FILTER, "UTF-8")
                    + "&result_type=recent&lang=es&since_id=" + lastId + "&count=100");
            connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Host", "api.twitter.com");
            connection.setRequestProperty("User-Agent", "TwitterDataSource");
            connection.setRequestProperty("Authorization", "Bearer " + bearerToken);
            connection.setUseCaches(false);
            JSONObject resp = (JSONObject) JSONValue.parse(readResponse(connection));
            if (resp != null) {
                tweetArr = (JSONArray) resp.get("statuses");
            }
        } catch (MalformedURLException e) {
            throw new IOException("Invalid endpoint URL specified.", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        if (tweetArr != null) {
            LOGGER.info("Tweets: " + tweetArr.size() + " tweets");
        }
        return tweetArr;
    }

    private String readResponse(HttpURLConnection connection) throws IOException {
        BufferedReader br = null;
        try {
            StringBuilder str = new StringBuilder();
            br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            while ((line = br.readLine()) != null) {
                str.append(line).append(System.getProperty("line.separator"));
            }
            return str.toString();
        } catch (IOException e) {
            return "";
        } finally {
            if (br != null) {
                br.close();
            }
        }
    }

    private String requestBearerToken() throws IOException {
        String bearerToken = null;
        HttpsURLConnection connection = null;
        try {
            String encodedConsumerKey = URLEncoder.encode(CONSUMER_KEY, "UTF-8");
            String encodedConsumerSecret = URLEncoder.encode(CONSUMER_SECRET, "UTF-8");
            String fullKey = encodedConsumerKey + ":" + encodedConsumerSecret;
            byte[] encodedBytes = Base64.encodeBase64(fullKey.getBytes());
            String encodedCredentials = new String(encodedBytes);

            URL url = new URL(ENDPOINT_AUTHENTICATION);
            connection = (HttpsURLConnection) url.openConnection();
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Host", "api.twitter.com");
            connection.setRequestProperty("User-Agent", "TwitterDataSource");
            connection.setRequestProperty("Authorization", "Basic " + encodedCredentials);
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded;charset=UTF-8");
            connection.setRequestProperty("Content-Length", "29");
            connection.setUseCaches(false);
            writeRequest(connection, "grant_type=client_credentials");

            // Parse the JSON response into a JSON mapped object to fetch fields from.
            JSONObject obj = (JSONObject) JSONValue.parse(readResponse(connection));
            if (obj != null) {
                String tokenType = (String) obj.get("token_type");
                String token = (String) obj.get("access_token");
                bearerToken = ((tokenType.equals("bearer")) && (token != null)) ? token : null;
            }
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
        LOGGER.info("BearerToken: " + bearerToken);
        return bearerToken;
    }

    private void writeRequest(HttpURLConnection connection, String textBody) throws IOException {
        try (BufferedWriter wr = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream()))) {
            wr.write(textBody);
            wr.flush();
        }
    }

    public void run() {
        try {
            // get Twitter bareerToken
            String bearerToken = requestBearerToken();

            // create a Kafka producer
            KafkaProducer<String, String> producer = createKafkaProducer();

            // add a shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                producer.close();
                LOGGER.info("Producer Closed");
            }));

            // loop to collect tweets
            long lastId = 0;
            while (true) {
                JSONArray msgArr = getTweets(bearerToken, lastId);
                if (msgArr != null) {
                    for (Object aMsgArr : msgArr) {
                        JSONObject tweet = (JSONObject) aMsgArr;
                        long tweetId = Long.parseLong(tweet.get("id_str").toString());
                        if (tweetId > lastId) {
                            lastId = tweetId;
                        }
                        String msg = tweet.toString();
                        // LOGGER.info(msg);
                        producer.send(new ProducerRecord<>(KAFKA_TOPIC, null, msg), new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                                if (e != null) {
                                    LOGGER.error("Something bad happened", e);
                                }
                            }
                        });
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(2);
                } catch (InterruptedException e) {
                    LOGGER.info("InterruptedException: " + e.getMessage());
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Exception: " + ex.getMessage());
        }
    }
}
