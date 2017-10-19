package Generator;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.io.ByteArrayOutputStream;
import java.util.Properties;
import Msg.Msg;
public class Generator {
    private static final Kryo KRYO = new Kryo();
    public static void main(String[] args) throws InterruptedException {


        String topicName = "MSG";

        Properties props = new Properties(){{
            put("bootstrap.servers", "localhost:9092");
            put("acks", "all");
            put("retries", 1);
            put("linger.ms", 10); // how long kafka accumulates messages in one batch
            put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
            put("value.serializer", org.apache.kafka.common.serialization.ByteArraySerializer.class.getName());
        }};
        Producer<String, byte[]> producer = new KafkaProducer<String,byte[]>(props);

        for (int i = 0; i < 10; i++) {
            Msg msg = new Msg();
            producer.send(new ProducerRecord<String,byte[]>(topicName, serialize(msg)));
            Thread.sleep(3000);
        }
        System.out.println("Messages sent successfully");
        producer.close();
    }

    public static byte[] serialize(Msg object) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);
        KRYO.writeClassAndObject(objOutput, object);
        objOutput.close();
        return objStream.toByteArray();
    }
}
