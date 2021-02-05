package demos;

import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.twitter.*;
import twitter4j.Status;

public class TweetStream {
	public static void main(String[] args) {
        final String consumerKey = "";
        final String consumerSecret = "";
        final String accessToken = "";
        final String accessTokenSecret = "";

        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("SparkTwitterAnalysis");
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(30000));
        
        String[] filters= {"CORONA","COVID19","CORONAVIRUS","Lockdown"};
        
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        
        
        
        
        JavaReceiverInputDStream<Status> twitterStream = TwitterUtils.createStream(jssc,filters);
        JavaDStream<Status> enTweetsDStream = twitterStream.filter((status) -> "en".equalsIgnoreCase(status.getLang()));
       
        JavaDStream<String> statuses = enTweetsDStream.map(
                new Function<Status, String>() {
                    public String call(Status status) { return status.getText(); }
                }
        );
       
        enTweetsDStream.dstream() .saveAsTextFiles("/output/tweets", "txt");

        statuses.print();
        jssc.start();
    }
}
