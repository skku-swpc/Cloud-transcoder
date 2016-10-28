package CloudTranscoding;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class CloudTranscoder {

	public void CloudTranscode() throws InterruptedException {
		// TODO Auto-generated method stub

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("RequestReader", new RequestReader());
		builder.setBolt("Transcoder", new Transcoder(), 40)
				.shuffleGrouping("RequestReader");
		
		// Configuration
		Config conf = new Config();	
		
		conf.setNumWorkers(42);
		// conf.put("wordsFile", "/data/storm/words.txt");
		conf.setDebug(true);
		// Topology run
		conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		
		// StormSubmitter
		//LocalCluster cluster = new LocalCluster();
		try {
			//cluster.submitTopology("Cloud-Transcoder", conf,
			StormSubmitter.submitTopology("Cloud-Transcoder", conf,
					builder.createTopology());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			System.out.println(e.toString());
		}
		
		//while(true);
		
		//cluster.shutdown();

	}

}
