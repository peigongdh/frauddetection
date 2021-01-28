package kafkaprometheustest;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;

public class NewUserAdditionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        env
                .addSource(new FlinkKafkaConsumer<>("test-flink", new JSONKeyValueDeserializationSchema(true), properties))
                .process(new NewUserAddition())
                .name("NewUserAdditionStream")
                .addSink(new PrometheusSink()).name("NewUserAdditionSink");

        env.execute("NewUserAddition");
    }

}
