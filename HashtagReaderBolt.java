

import java.io.*;
import java.util.HashMap;
import java.util.Map;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class HashtagReaderBolt implements IRichBolt {
   private OutputCollector collector;
   private PrintWriter _log;
   private Status tweet;
   
   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      this.collector = collector;      
      try{
		_log = new PrintWriter(new File("/s/chopin/b/grad/hari7988/xyz/hashtaglog.txt"));
		}catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 2 ");
			e.printStackTrace();
		}
      
   }

   
   public void execute(Tuple tuple) {
	  
	  int sentiment = (int) tuple.getValueByField("sentimentScore");
      this.tweet = (Status) tuple.getValueByField("tweet");
      for(HashtagEntity hashtag : tweet.getHashtagEntities()) {
         _log.write(" Hashtag: " + hashtag.getText() + " 	Sentiment Score: " + sentiment + "\n");
         _log.flush();
         this.collector.emit(new Values(hashtag.getText(), sentiment));

      }
   }

   
   public void cleanup() {}

   
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("hashtag", "sentimentScore"));
   }
	
   
   public Map<String, Object> getComponentConfiguration() {
      return null;
   }
	
}
