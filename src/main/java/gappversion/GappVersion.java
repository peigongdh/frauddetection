package gappversion;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

public class GappVersion extends ProcessFunction<ObjectNode, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(GappVersion.class);

    @Override
    public void processElement(ObjectNode value, Context context, Collector<String> collector) throws Exception {
//        if (LocalDateTime.now().getSecond() % 10 == 0) {
//            LOG.info(value.toString());
//        }
        LOG.info(value.toString());
    }
}
