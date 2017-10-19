package Stub;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import Msg.Msg;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
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
            String id;
            Date date;
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(1);
                for (ConsumerRecord<String, byte[]> record : records) {
                    Msg msg = deserialize(record.value());
                    id = msg.getId();
                    date = msg.getDate();
                    System.out.println(id + " " + date);
                    fileWriter.write(id + ";" + date + System.lineSeparator());
                    fileWriter.flush();
                    Thread.sleep(40000);
                    Producer<String, byte[]> answerProd = new KafkaProducer<String, byte[]>(props);
                    answerProd.send(new ProducerRecord<String, byte[]>("response", serialize(new Response(id, "OK"))));
                }
            }
        } catch (IOException e) {
            System.out.println(e);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static Msg deserialize(byte[] data) {
        return (Msg) KRYO.readClassAndObject(new Input(data));
    }

    public static byte[] serialize(Response object) {
        ByteArrayOutputStream objStream = new ByteArrayOutputStream();
        Output objOutput = new Output(objStream);
        KRYO.writeClassAndObject(objOutput, object);
        objOutput.close();
        return objStream.toByteArray();
    }
}