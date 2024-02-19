package co.zhanglintc;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        System.out.println(App.class.getClassLoader().getResource(""));
        System.out.println(App.class.getResource(""));
        System.out.println("Hello World!");

        Properties props = new Properties();
        InputStream in = App.class.getClassLoader().getResourceAsStream("kafka.properties");
        props.load(in);
        in.close();

        String topicName = "news";

        Thread t = new Thread(() -> {
            KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topicName));
            System.out.println("Con done");
            Map<String, List<PartitionInfo>> stringListMap = consumer.listTopics();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                if (!records.isEmpty()) {
                    break;
                }
            }
            consumer.close();
        });
        t.setDaemon(false);
        t.start();

        Thread.sleep(1000 * 1);

        Producer<String, String> producer = new KafkaProducer<>(props);
        for (int i = 0; i < 100; i++)
            producer.send(new ProducerRecord<String, String>(topicName, null, Integer.toString(i)));

        producer.close();
        System.out.println("Pro done");
    }
}
