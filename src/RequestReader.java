package CloudTranscoding;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import redis.clients.jedis.Jedis;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class RequestReader implements IRichSpout {

	private SpoutOutputCollector collector;

	private Queue<String> messages;
	
	@Override
	public void nextTuple() {
		while (!messages.isEmpty()) {
			String request = messages.poll();

			String resolution = request.split("_")[0];
			String name = request.split("_")[1];
			
			System.out.println("next tuple resolution :  " + resolution + " name : " + name);

			collector.emit(new Values(resolution, name));
		}
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		messages = new LinkedList<String>();

		new Thread(new Runnable() {
			@Override
			public void run() {
				Jedis client = new Jedis("localhost");
				while (true) {
					try {				
						List<String> res = client.blpop(Integer.MAX_VALUE, "storm_request_queue");
						messages.offer(res.get(1));
					} catch (Exception e) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException e1) {
						}
					}
				}

			}
		}).start();

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("resolution", "name"));
	}

	@Override
	public void close() {
	}

	@Override
	public void activate() {
	}

	@Override
	public void deactivate() {
	}

	@Override
	public void ack(Object msgId) {
		System.out.println("OK:" + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL:" + msgId);
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}