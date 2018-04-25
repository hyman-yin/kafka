package hyman.kafka.producer;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyKafkaProducer {
	private static Producer<Integer,String> producer;
	private static final Properties props = new Properties();
	
	public static void main(String[] args) {
		props.put("bootstrap.servers", "192.168.106.126:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		
		producer = new KafkaProducer<Integer, String>(props);
		
		for(int i=0;i<100;i++){
			producer.send(new ProducerRecord<Integer, String>("test4", Integer.toString(i)));
			try {
				TimeUnit.SECONDS.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		producer.close();
		
		System.out.println("send over......");
	}
	
}
