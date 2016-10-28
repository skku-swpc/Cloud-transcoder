package CloudTranscoding;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class Transcoder implements IRichBolt {

	class FfmpegThread extends Thread {
		public InputStream is;
		public OutputStream os;
		byte[] buffer;

		FfmpegThread(OutputStream os, byte[] buffer) {
			this.os = os;
			this.buffer = buffer;
		}

		public void run() {

			try {
				os.write(buffer);
				os.flush();
				os.close();

				// proc.waitFor();

			} catch (Exception e) {

			}

		}
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
	}

	@Override
	public void execute(Tuple input) {

		try {

			String resolution = input.getString(0);
			String name = input.getString(1);

			JedisPool pool;
			
			pool = new JedisPool(new JedisPoolConfig(), "localhost");
			Jedis jedis = pool.getResource();

			Runtime rt = Runtime.getRuntime();
			
			//String cmd = "ffmpeg -i - -s " + "800x640"+ " -f mpegts -";
			String cmd = "ffmpeg -i - -s " + resolution + " -f mpegts -";
			
			Process proc = rt.exec(cmd);
			
			OutputStream os = proc.getOutputStream();
			InputStream is = proc.getInputStream();
			
			byte[] reference_video = jedis.get(name.getBytes());
			//byte[] reference_video = jedis.get("reference".getBytes());
			
			FfmpegThread ffmpegThread = new FfmpegThread(os, reference_video);
			ffmpegThread.start();

			byte[] buffer = new byte[10240]; // what is best array size to
												// read

			int len = 0;

			ByteArrayOutputStream arrays = new ByteArrayOutputStream();

			// is.read(b, off, len)
			while ((len = is.read(buffer)) != -1) {
				arrays.write(buffer, 0, len);
			}

			jedis.set((resolution + "_" + name).getBytes(), arrays.toByteArray());
			
			jedis.expire((resolution + "_" + name).getBytes(), 3600);

			is.close();
	
			proc.waitFor();
			
			pool.returnResource(jedis);
			pool.destroy();

		} catch (Exception e) {
			System.out.println("[arcs]" + e.toString());
		}

	}

	@Override
	public void cleanup() {
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}