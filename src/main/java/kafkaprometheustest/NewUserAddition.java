package kafkaprometheustest;

import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewUserAddition extends KeyedProcessFunction<String, String, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NewUserAddition.class);

    @Override
    public void processElement(String s, Context context, Collector<String> collector) throws Exception {
        collector.collect(s);
        LOG.info(s);
    }
}
