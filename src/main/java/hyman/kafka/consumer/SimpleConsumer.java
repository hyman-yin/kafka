package hyman.kafka.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

@SuppressWarnings("deprecation")
public class SimpleConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("zookeeper.connect", "192.168.106.126:2181");
		props.put("group.id", "g1");
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		
		ConsumerConfig config = new ConsumerConfig(props);
		ConsumerConnector connector = Consumer.createJavaConsumerConnector(config);
		
		Map<String, Integer> map = new HashMap<>();
		map.put("test5", 1);
		Map<String, List<KafkaStream<byte[], byte[]>>> streamMap = connector.createMessageStreams(map);
		
		List<KafkaStream<byte[], byte[]>> list = streamMap.get("test5");
		
		for(KafkaStream<byte[], byte[]> stream : list){
			ConsumerIterator<byte[], byte[]> it = stream.iterator();
			while(it.hasNext()){
				String msg = new String(it.next().message());
				System.out.println(msg);
			}
		}
		connector.shutdown();
	}
}
