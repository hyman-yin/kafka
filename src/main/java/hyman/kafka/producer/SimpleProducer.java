package hyman.kafka.producer;

import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * kafka 简单生产者实例，如果抛异常无法连接kafka，检查各项配置，检查kafka/config/server.xml中是否配置hostname
 * @author hyman
 *
 */

@SuppressWarnings("deprecation")
public class SimpleProducer {
	private static Producer<Integer,String> producer;
	private static final Properties props = new Properties();
	
	public static void main(String[] args) {
		props.put("metadata.broker.list", "192.168.106.126:9092");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.acks", "1");
		
		producer = new Producer<>(new ProducerConfig(props));
		
		String topic = "test3";
		String message = "hello world";
		KeyedMessage<Integer,String> data = new KeyedMessage<Integer, String>(topic, message);
		producer.send(data);
		producer.close();
		
		System.out.println("send over......");
	}
	
}
