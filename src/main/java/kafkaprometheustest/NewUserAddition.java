package kafkaprometheustest;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewUserAddition extends ProcessFunction<ObjectNode, String> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(NewUserAddition.class);

    @Override
    public void processElement(ObjectNode value, Context context, Collector<String> collector) {
        JsonNode isNew = value.get("value").get("is_new");
        if (isNew == null || !"true".equals(isNew.asText())) {
            return;
        }
        JsonNode app = value.get("value").get("app");
        if (app == null || app.isNull()) {
            return;
        }
        collector.collect(app.asText());
        LOG.info(app.asText());
    }
}
