


import java.util.Arrays;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import org.apache.storm.StormSubmitter;

import org.apache.log4j.xml.DOMConfigurator;
import org.apache.log4j.PropertyConfigurator;


public class TwitterTopology {        
    public static void main(String args[]) throws Exception{
        String consumerKey = "Ozu44TRF7Y0goJOuQ3ynxpGLF"; 
        String consumerSecret = "i3Koq8qd3oFlUWGjzklY3nL3nhICtOpyssUyvhXwvALPyuK1s0"; 
        String accessToken = "26958612-tl0u6Fg3ThxtRSmuDm6Yy5VbvN425aG5EgCSTyHto"; 
        String accessTokenSecret = "sTnRdnwGMHAcdRLY7DOkZk885aDbjrVPH8d5vGxDble5M";
        

        
        TopologyBuilder builder = new TopologyBuilder();
        
//----------------------------Spout------------------------------------------------------------------
        
        builder.setSpout("twitter-spout", new TwitterSampleSpout(consumerKey,
                consumerSecret, accessToken, accessTokenSecret));
        
//----------------------Sentiment Analysis ----------------------------------------------------------        
        builder.setBolt("twitter-sentiment-analysis", new SentimentAnalysisBolt())
        .shuffleGrouping("twitter-spout");
        
//------------------------Hashtag Reader-------------------------------------------------------------
        
//        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(),)
 //       .shuffleGrouping("twitter-sentiment-analysis");
        
        builder.setBolt("twitter-hashtag-reader-bolt", new HashtagReaderBolt(), 10)
        .shuffleGrouping("twitter-sentiment-analysis");

//----------------------Named Entity Reader----------------------------------------------------------        
        
//        builder.setBolt("twitter-named-entity-bolt", new NamedEntityBolt())
//        .shuffleGrouping("twitter-sentiment-analysis");
        
        builder.setBolt("twitter-named-entity-bolt", new NamedEntityBolt(), 10)
        .shuffleGrouping("twitter-sentiment-analysis");
             
//---------------------------Lossy Count Hashtags----------------------------------------------------       
        builder.setBolt("hashtag-lossy-count-bolt", new LossyHashBolt(), 10)
        .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag","sentimentScore"));
/*        
        
        builder.setBolt("hashtag-lossy-count-bolt", new LossyHashBolt())
        .fieldsGrouping("twitter-hashtag-reader-bolt", new Fields("hashtag","sentimentScore"));      
        
 */       
//------------------------- Lossy Count Named Entity--------------------------------------------------        
                
        builder.setBolt("NamedEntity-lossy-count-bolt", new LossyNEBolt(), 10)
        .fieldsGrouping("twitter-named-entity-bolt", new Fields("NamedEntity","sentimentScore"));      
 /*        
        builder.setBolt("NamedEntity-lossy-count-bolt", new LossyNEBolt())
        .fieldsGrouping("twitter-named-entity-bolt", new Fields("NamedEntity","sentimentScore"));
*/
//-----------------------------Hashtag Report---------------------------------------------------------        
        builder.setBolt("report-hashtag-bolt", new ReportBolt(), 10)
        .globalGrouping("hashtag-lossy-count-bolt"); 
/*
        builder.setBolt("report-hashtag-bolt", new ReportBolt())
        .globalGrouping("hashtag-lossy-count-bolt");
*/        
                
//----------------------------Named Entity Report-----------------------------------------------------        
        builder.setBolt("report-named-entity-bolt", new ReportNEBolt(), 10)
        .globalGrouping("NamedEntity-lossy-count-bolt");        
/*        
        builder.setBolt("report-named-entity-bolt", new ReportNEBolt())
        .globalGrouping("NamedEntity-lossy-count-bolt");        
 */      
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(10);
        
        StormSubmitter.submitTopology("test", conf, builder.createTopology());
        
 /*       
        LocalCluster cluster = new LocalCluster();
        
        cluster.submitTopology("test", conf, builder.createTopology());
*/        
//        Utils.sleep(10000);
//        cluster.shutdown();
    }
}
