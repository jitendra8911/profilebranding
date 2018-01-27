import org.apache.spark.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import scala.Tuple2;
import twitter4j.GeoLocation;
import twitter4j.Status;

import java.util.List;

public class App {
    public static void main(String[] args) {
        final String consumerKey = "jSyvYVibMGe0XeTcJcyuHBaBk";
        final String consumerSecret = "qI8vzHPmb6pD6jOy7P1hSoQTaHhtDmhCShu49X3j6TXM2kd0mF";
        final String accessToken = "956981340062277633-DBnpNmIuP0bBfCj4qHLkUrSd5sgTtwG";
        final String accessTokenSecret = "50CiRGCY9oqrFPSnvfpOFUAUUe4GRVK7AVl7uuZTxgGE7";
        KafkaProducerExample kafkaProducer = new KafkaProducerExample();

        String word = "the";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterHelloWorldExample");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(20000));

        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);

        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc);

        // Without filter: Output text of all tweets
//        JavaDStream<String> statuses = twitterStream.map(
//                new Function<Status, String>() {
//                    public String call(Status status) { return status.getText(); }
//                }
//        );

        JavaDStream<Tuple2<Long, String>> validStatuses = twitterStream.filter(tweet -> tweet.getText().contains(word))
                .map(status -> new Tuple2<>(status.getRetweetCount(), status.getText()));
;
        // With filter: Only use tweets with geolocation and print location+text.
        /*JavaDStream<Status> tweetsWithLocation = twitterStream.filter(
                new Function<Status, Boolean>() {
                    public Boolean call(Status status){
                        if (status.getGeoLocation() != null) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
        );
        JavaDStream<String> statuses = tweetsWithLocation.map(
                new Function<Status, String>() {
                    public String call(Status status) {
                        return status.getGeoLocation().toString() + ": " + status.getText();
                    }
                }
        );*/

        validStatuses.foreachRDD(
                new Function<JavaRDD<Tuple2<Long, String>>, Void>() {
                    @Override
                    public Void call(JavaRDD<Tuple2<Long, String>> tuple2JavaRDD) throws Exception {
                        tuple2JavaRDD.collect().forEach(tuple -> kafkaProducer.sendMessage(tuple));
                        return null;
                    }
                }
        );

        jssc.start();
    }
}
