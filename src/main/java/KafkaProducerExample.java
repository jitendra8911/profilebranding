
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.Tuple2;

import java.util.Properties;

public class KafkaProducerExample {

    private static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        return new KafkaProducer(props);
    }

    final Producer<Long, String> producer = createProducer();

    public void sendMessage(Tuple2<Long, String> message) {
        try {
            final ProducerRecord<Long, String> record = new ProducerRecord("test", null, message);
            producer.send(record);
        } finally {
            producer.flush();
        }
    }
}
