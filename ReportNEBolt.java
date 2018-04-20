
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.TreeMap;
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


public class ReportNEBolt extends BaseRichBolt {

	private HashMap<Double, List<LossyObj>> counts=null; 
	private double start_time;
	private double curr;
	private PrintWriter _log;
	//private static final Logger logger = LoggerFactory.getLogger(ReportingBolt.class);
	DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
	
	

	public void prepare(Map config, TopologyContext context,
			OutputCollector collector) {
		this.counts = new HashMap<Double,List<LossyObj>>();
		start_time=new Date().getTime()/1000;
		System.out.println("____________________________Report BOLT being created_______________________________________________");
	      try{
	  		_log = new PrintWriter(new File("/s/chopin/b/grad/hari7988/xyz/NEReport.txt"));
	  		}catch (Exception e) {
	  			System.out.println("UNABLE TO WRITE FILE :: 4 ");
	  			e.printStackTrace();
	  		}
	}

	public void execute(Tuple tuple) {
		curr = new Date().getTime() / 1000;
		String word = tuple.getStringByField("NamedEntity");
		int count = tuple.getIntegerByField("count");
		int loss_count = tuple.getIntegerByField("lossy_count");
		int sentimentScore=(int)tuple.getValueByField("sentimentScore");
		LossyObj element = new LossyObj();
		element.element = word;
		element.freq = count;
		element.delta = loss_count;
		element.timestamp = curr;
		element.sentimentScore=sentimentScore;
		if (counts.containsKey(curr)) {
			List<LossyObj> list = counts.get(curr);
			list.add(element);
			counts.put(curr, list);
		} else {
			List<LossyObj> list = new ArrayList<LossyObj>();
			list.add(element);
			counts.put(curr, list);
		}


		if (curr - start_time >= 10) {// will display output every 10 seconds {
			Output_Display();
			start_time = new Date().getTime() / 1000;
		}
	}


	public void Output_Display()
	{
		System.out.println("Displays after every 10 seconds");
		System.out.println("Start time: "+String.format("%12.0f",start_time));
		System.out.println("--- DIFFERENCE: "+ (curr-start_time)+" ---"+String.format("%12.0f",curr));
		System.out.println("Below are the top Named Entity");
		HashMap<String, LossyObj> counts_mid= new HashMap<String,LossyObj>(); 
		List<Double> keys = new ArrayList<Double>(counts.keySet());
		Calendar cal = Calendar.getInstance();


		for (double key : keys) {
			if(key<=start_time+10)
			{
				List<LossyObj> objs = counts.get(key);
				for(LossyObj obj : objs) {
					if(counts_mid.get(obj.element)!=null)
					{
						if(counts_mid.get(obj.element).timestamp<obj.timestamp)
						{
							counts_mid.put(obj.element,obj);
						}
					}
					else
					{
						counts_mid.put(obj.element,obj);
					}

				}
				counts.remove(key);
			}
		}

		Map<Integer, List<LossyObj>> entryCount = new TreeMap<Integer, List<LossyObj>>();
		for(Map.Entry<String, LossyObj> entry : counts_mid.entrySet()) {
			if(entryCount.get(entry.getValue().freq) != null) {
				List<LossyObj> oList =  entryCount.get(entry.getValue().freq);
				oList.add(entry.getValue());
				entryCount.put(entry.getValue().freq, oList);
			} else {
				List<LossyObj> oList = new ArrayList<LossyObj>();
				oList.add(entry.getValue());
				entryCount.put(entry.getValue().freq, oList);
			}

		}

		/*SORTED MAP IS SORTED BASED ON COUNT*/
		List<Integer> keySet = new ArrayList<Integer>(entryCount.keySet());
		Collections.sort(keySet, Collections.reverseOrder());
		List<String> printedTags = new ArrayList<String>();

		_log.write(("Time : "+dateFormat.format(cal.getTime())+"\t" )+"\n");
		for (Integer entry : keySet) {
			for(LossyObj o : entryCount.get(entry)) {
				System.out.println(o.element+" : "+o.freq+" --- "+o.freq);
				//logger.info("Hashtag:"+"\t"+o.element+"\n"+"Frequency: "+"\t"+o.freq+"\n");
				_log.write("Named Entity:"+"\t"+o.element+"\n"+"Frequency: "+"\t"+o.freq+"\n"+"Sentiment score"+"\t"+o.sentimentScore+"\n");
				_log.flush();
			}
		}
	
		System.out.println("--------------");
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) { // this
		// bolt
		// does
		// not
		// emit
		// anything
	}


}
