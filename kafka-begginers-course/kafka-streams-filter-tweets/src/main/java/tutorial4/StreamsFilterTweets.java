package tutorial4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class StreamsFilterTweets {
    public static void main(String[] args) {
        //create properties
        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "demo-kafka-streams");
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());


        // create a topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream("Twitter_tweets");
        KStream<String, String> filteredStream = inputTopic.filter(
                (k, jsonTweet) -> extractUserFollowersInTweet(jsonTweet) > 10000
                //filter for tweets which has a user of over 10000 followers
        );
        filteredStream.to("important_tweets");
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(), props);

        //start our streams application
        kafkaStreams.start();
    }

    private static JsonParser jsonParser = new JsonParser();

    private static int extractUserFollowersInTweet(String tweetJson) {
        //gson Library
        try {

            int id = jsonParser.parse(tweetJson).getAsJsonObject().get("user").getAsJsonObject().get("followers_count").getAsInt();
            return id;
        } catch (NullPointerException e) {
            return 0;
        }
    }
}
