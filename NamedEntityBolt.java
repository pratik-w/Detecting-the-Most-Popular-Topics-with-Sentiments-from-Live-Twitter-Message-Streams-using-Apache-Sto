

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.*;

import twitter4j.*;
import twitter4j.conf.*;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import edu.stanford.nlp.ie.AbstractSequenceClassifier;
import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.ling.CoreAnnotations.NamedEntityTagAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.PartOfSpeechAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TextAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations.TokensAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class NamedEntityBolt implements IRichBolt {
	   private OutputCollector collector;
	   private Properties prop;
	   private StanfordCoreNLP pipeline;
	   private Status tweet;
	   private PrintWriter _log;
	   public final static Pattern pat1 = Pattern.compile("<LOCATION>(.+?)</LOCATION>");
	   public final static Pattern pat2 = Pattern.compile("<PERSON>(.+?)</PERSON>");
	   public final static Pattern pat3 = Pattern.compile("<ORGANIZATION>(.+?)</ORGANIZATIOn>");	   
	   public final static Pattern pat4 = Pattern.compile("<DATE>(.+?)</DATE>");
	   public final static Pattern pat5 = Pattern.compile("<MONEY>(.+?)</MONEY>");
	   public final static Pattern pat6 = Pattern.compile("<PERCENT>(.+?)</PERCENT>");
	   public final static Pattern pat7 = Pattern.compile("<TIME>(.+?)</TIME>");
	
	 public void prepare(Map conf, TopologyContext context, OutputCollector collector){
	      this.collector = collector;
	      this.prop = new Properties();
	      this.prop.setProperty("annotators", "tokenize, ssplit, pos, parse, sentiment");
	      this.pipeline = new StanfordCoreNLP(prop);
	      try{
	    	  _log = new PrintWriter(new File("/s/chopin/b/grad/hari7988/xyz/NamedEntity.txt"));
	  		}catch (Exception e) {
	  			System.out.println("UNABLE TO WRITE FILE :: 3 ");
	  			e.printStackTrace();
	  		}
	        	      
	   }
	 
	 
	 public void execute(Tuple tuple) {
		 this.tweet = (Status) tuple.getValueByField("tweet");
		 int sentiment = (int) tuple.getValueByField("sentimentScore");
		 CoreLabel  corelabel;
		 String serializedClassifier =
					"edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
		 AbstractSequenceClassifier<CoreLabel> classifier = null;
			try {
				classifier = CRFClassifier.getClassifier(serializedClassifier);
			} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}		 
		String text = tweet.getText();
		String temp = classifier.classifyToString(text, "inlineXML", false);
		for (int i=0; i < getTagValues(temp).size(); i++){
			_log.write(" Named Entity: " + getTagValues(temp).get(i) + "    	Sentiment Score: " + sentiment  + "\n");
			_log.flush();			
			this.collector.emit(new Values(getTagValues(temp).get(i), sentiment));
		}
				 
		 
	 }
	 

	 private static List<String> getTagValues(final String str) {
		    final List<String> tagValues = new ArrayList<String>();
		    final Matcher matcher1 = pat1.matcher(str);
		    final Matcher matcher2 = pat2.matcher(str);
		    final Matcher matcher3 = pat3.matcher(str);
		    final Matcher matcher4 = pat4.matcher(str);
		    final Matcher matcher5 = pat5.matcher(str);
		    final Matcher matcher6 = pat6.matcher(str);
		    final Matcher matcher7 = pat7.matcher(str);
		    while (matcher1.find()) {
		        tagValues.add(matcher1.group(1));
		    }
		    while (matcher2.find()) {
		        tagValues.add(matcher2.group(1));
		    }
		    while (matcher3.find()) {
		        tagValues.add(matcher3.group(1));
		    }
		    while (matcher4.find()) {
		        tagValues.add(matcher4.group(1));
		    }		    
		    while (matcher5.find()) {
		        tagValues.add(matcher5.group(1));
		    }
		    while (matcher6.find()) {
		        tagValues.add(matcher6.group(1));
		    }
		    while (matcher7.find()) {
		        tagValues.add(matcher7.group(1));
		    }
		    return tagValues;
		}
		
	 
	 /*
	 
	 public void execute(Tuple tuple) {
		 this.tweet = (Status) tuple.getValueByField("tweet");
		 int sentiment = (int) tuple.getValueByField("sentimentScore");
		 String curToken = "0";
		 String word = "0";
/*		 String serializedClassifier =
			"edu/stanford/nlp/models/ner/english.muc.7class.distsim.crf.ser.gz";
		 AbstractSequenceClassifier<CoreLabel> classifier = null;
			try {
				classifier = CRFClassifier.getClassifier(serializedClassifier);
			} catch (IOException | ClassNotFoundException e) {
					e.printStackTrace();
				}	

		 Annotation document = new Annotation(this.tweet.getText());
		 pipeline.annotate(document);
		 List<CoreMap> sentences = document.get(SentencesAnnotation.class);
		   for (CoreMap sentence: sentences) {
				for(CoreLabel token : sentence.get(TokensAnnotation.class)){
					curToken =  token.get(NamedEntityTagAnnotation.class);
//					String pos = token.get(PartOfSpeechAnnotation.class);
					word = token.get(TextAnnotation.class);
					if(curToken.length() > 1){
						_log.write(" Named Entity: " + word + "		Entity: " + curToken + "   	Sentiment Score: " + sentiment + "\n");
						_log.flush();
						this.collector.emit(new Values(word, sentiment));
					  }
					}
				
				   
			}	 
	  }	   
*/	
	 public void cleanup() {}

	   
	 public void declareOutputFields(OutputFieldsDeclarer declarer) {
	    declarer.declare(new Fields("NamedEntity", "sentimentScore"));
	 }
	 
	 @Override
	 public Map<String, Object> getComponentConfiguration() {
	     return null;
	 }
	
}