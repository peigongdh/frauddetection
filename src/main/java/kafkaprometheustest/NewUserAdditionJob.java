package kafkaprometheustest;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class NewUserAdditionJob {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("group.id", "test");

        DataStreamSource<ObjectNode> streamSource = env
                .addSource(new FlinkKafkaConsumer<>("test-flink", new JSONKeyValueDeserializationSchema(true), properties));
        DataStream<String> dataStream = streamSource.process(new NewUserAddition())
                .name("NewUserAdditionStream");
        dataStream.addSink(new PrometheusSink()).name("NewUserAdditionSink");

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3,
                Time.of(5, TimeUnit.SECONDS)
        ));
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.execute("NewUserAddition");
    }

}
