

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
//import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;
import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.Paging;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;


public class LossyHashBolt extends BaseRichBolt{
	
	private OutputCollector collector;
	private HashMap<String, Long> counts = null;
	
	//declaring and initializing variables for lossy counting
	private Map<String, LossyObj> window = new ConcurrentHashMap<String,LossyObj>();
	private double e=0.05f;
	private int bucket_size=15;
	private int curr_bucket=1;
	private int number_of_elements=0;
	

	@Override
	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("hashtag");
		int sentiment = (int) tuple.getValueByField("sentimentScore");
		lossy_counting(word, sentiment);
	}
	
	public void lossy_counting(String hashtag, int sentiment)
	{
		
		if(number_of_elements<bucket_size) {
			if(!window.containsKey(hashtag)) {
				LossyObj d = new LossyObj();
				d.delta = curr_bucket-1;
				d.sentimentScore = sentiment;
				d.element = hashtag;
				window.put(hashtag, d);
			}
			else {
			 LossyObj d = window.get(hashtag);
				d.freq+=1;
				window.put(hashtag, d);
			}
			number_of_elements+=1;
		}
		if( number_of_elements == bucket_size) {
			Delete();
			for(String str_loss: window.keySet()) {
				LossyObj d = window.get(str_loss);
				
				collector.emit(new Values(str_loss, d.freq, d.freq + d.delta, d.sentimentScore));
			}
			number_of_elements=0;
			curr_bucket+=1;
		}
	}
		
	
	
	
	public void Delete()
	{
		for(String hashtag: window.keySet()) {
			LossyObj d = window.get(hashtag);
			double freqandDelta = d.freq + d.delta;
			if(freqandDelta <= curr_bucket) {
				window.remove(hashtag);
			}
		}
	}
	

	@Override
	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("hashtag", "count","lossy_count", "sentimentScore"));
	}

}
