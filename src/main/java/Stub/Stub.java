package Stub;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import Msg.Msg;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

public class Stub {
    private static final Kryo KRYO = new Kryo();

    public static void main(String[] args) {
        String topicName = "MSG";

        Properties props = new Properties() {{
            put("bootstrap.servers", "localhost:9092");
            put("group.id", "MSG");
            put("enable.auto.commit", true);
            put("auto.offset.reset", "earliest");
            put("auto.commit.interval.ms", 1000);
            put("session.timeout.ms", 30000);
            put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            put("value.deserializer", org.apache.kafka.common.serialization.ByteArrayDeserializer.class.getName());
        }};
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
        consumer.subscribe(Arrays.asList(topicName));
        System.out.println("Subscribed to topic " + topicName);
        try (FileWriter fileWriter = new FileWriter("MSGs.csv", true);) {
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll( 1);
                for (ConsumerRecord<String, byte[]> record : records) {
                    Msg msg = deserialize(record.value());
                    System.out.println(msg.getId() + " " + msg.getDate());
                    fileWriter.write(msg.getId() + ";" + msg.getDate() + System.lineSeparator());
                    fileWriter.flush();
                }
            }
        } catch (IOException e) {
            System.out.println(e);
        }
    }

    public static Msg deserialize(byte[] data) {
        return (Msg) KRYO.readClassAndObject(new Input(data));
    }
}