

import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Properties;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.shade.org.eclipse.jetty.server.AbstractHttpConnection.Output;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;


import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
//import edu.stanford.nlp.pipeline.*;
//import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.*;
import edu.stanford.nlp.util.*;

import edu.stanford.nlp.pipeline.*;




public class SentimentAnalysisBolt implements IRichBolt {
	   private OutputCollector collector;
	   private Properties prop;
	   private StanfordCoreNLP pipeline;
	   private Status tweet;
	   private String tweettext;
	   private int sentimentScore = 0;
	   private PrintWriter _log;
	   
	   @Override
	   public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
	      this.collector = collector;
	      this.prop = new Properties();
	      this.prop.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
	      this.pipeline = new StanfordCoreNLP(prop);
	      try{
	  		_log = new PrintWriter(new File("/s/chopin/b/grad/hari7988/xyz/sentimentlog.txt"));
	  		}catch (Exception e) {
	  			System.out.println("UNABLE TO WRITE FILE :: 1 ");
	  			e.printStackTrace();
	  		}	        	      
	   }
	
	   public void execute(Tuple tuple) {

	   this.tweet = (Status) tuple.getValueByField("tweet");
//	   this.tweettext = "I am very hungry";
//	   System.out.println("Text: " + this.tweettext);
//	   System.out.println("Text: " + this.tweet.getText());
	   
	   Annotation document = new Annotation(this.tweet.getText());
//	   Annotation document = new Annotation(tweettext);
	   pipeline.annotate(document);
	   List<CoreMap> sentences = document.get(SentencesAnnotation.class);
	   for (CoreMap sentence: sentences) {
		   String sentiment = sentence.get(SentimentCoreAnnotations.SentimentClass.class).toLowerCase();
		   sentimentScore += calSentiment(sentiment);
		   System.out.println("Sentiment: " + sentiment);
		   if(sentimentScore > 2){
			   sentimentScore = 2;
		   }else if(sentimentScore < -2){
			   sentimentScore = -2;
		   }
	       _log.write(" Text: " + tweet.getText() + " 			Sentiment Score: " + sentimentScore +"\n");
	       _log.flush();		   
		   this.collector.emit(new Values(tweet, sentimentScore));
		}
//	   	System.out.println("Sentiment Score: " + sentimentScore);
	   }
	    
	   private int calSentiment(String sentiment){
		   
		   if (sentiment.contentEquals("very negative")){
			   return -2;
		   }
		   else if (sentiment.contentEquals("negative")){
			   return -1;
		   }   
		   else if (sentiment.contentEquals("neutral")){
			   return 0;
		   }
		   else if (sentiment.contentEquals("positive")){
			   return 1;
		   }
		   else if (sentiment.contentEquals("very positive")){
			   return 2;
		   }
		   else return 0;
	   }
	   
	   
	   @Override
	   public void cleanup() {}

	   @Override
	   public void declareOutputFields(OutputFieldsDeclarer declarer) {
	      declarer.declare(new Fields("tweet", "sentimentScore") );
	   }
		
	   @Override
	   public Map<String, Object> getComponentConfiguration() {
	      return null;
	   }
}
