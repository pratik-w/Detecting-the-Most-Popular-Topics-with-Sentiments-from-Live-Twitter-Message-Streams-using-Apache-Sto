

import java.io.IOException;
import java.util.*;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import org.apache.storm.Config;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


import edu.stanford.nlp.ling.CoreAnnotations.SentencesAnnotation;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations;
import edu.stanford.nlp.util.CoreMap;



@SuppressWarnings("serial")
public class TwitterSampleSpout extends BaseRichSpout {

	SpoutOutputCollector _collector;
	LinkedBlockingQueue<Status> queue = null;
	TwitterStream _twitterStream;
	String consumerKey;
	String consumerSecret;
	String accessToken;
	String accessTokenSecret;
//	String[] keyWords;

	public TwitterSampleSpout(String consumerKey, String consumerSecret,
			String accessToken, String accessTokenSecret) {
		this.consumerKey = consumerKey;
		this.consumerSecret = consumerSecret;
		this.accessToken = accessToken;
		this.accessTokenSecret = accessTokenSecret;
//		this.keyWords = keyWords;
	}
/*
	public TwitterSampleSpout() {
		// TODO Auto-generated constructor stub
	}
*/
	
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		_collector = collector;

		StatusListener listener = new StatusListener() {			
			public void onStatus(Status status) {		
				queue.offer(status);
			}

			public void onDeletionNotice(StatusDeletionNotice sdn) {}
			public void onTrackLimitationNotice(int i) {}
			public void onScrubGeo(long l, long l1) {}
			public void onException(Exception ex) {}
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
			}
		};

		ConfigurationBuilder builder = new ConfigurationBuilder();
		builder.setOAuthConsumerKey(consumerKey);
		builder.setOAuthConsumerSecret(consumerSecret);
		Configuration configuration = builder.build();
		_twitterStream = new TwitterStreamFactory(configuration)
				.getInstance();
		_twitterStream.addListener(listener);

		AccessToken token = new AccessToken(accessToken, accessTokenSecret);
		_twitterStream.setOAuthAccessToken(token);
		_twitterStream.sample("en");
		
	}
	
	public void nextTuple() {
		Status ret = queue.poll();
		if (ret == null) {
			Utils.sleep(1000);
			try{
				Thread.sleep(1000);
			}catch(Exception e)
			{
				e.printStackTrace();
			}
		} else {
			_collector.emit(new Values(ret));
//			System.out.println("****************SPOUT*********"+ret);
		}
	}

	public void close() {
		_twitterStream.shutdown();
	}
	
	public Map<String, Object> getComponentConfiguration() {
		Config ret = new Config();
		ret.setMaxTaskParallelism(1);
		return ret;
	}
	
	public void ack(Object id) {}

	
	public void fail(Object id) {}

	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}

}
