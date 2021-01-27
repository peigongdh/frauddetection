package kafkaprometheustest;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class NewUserAdditionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("test-flink", new SimpleStringSchema(), properties));

        DataStream<String> alerts = stream
                .keyBy(String::toString)
                .process(new NewUserAddition())
                .name("NewUserAdditionStream");

        alerts.addSink(new PrometheusSink()).name("NewUserAdditionSink");

        env.execute("NewUserAddition");
    }

}
