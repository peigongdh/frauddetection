package gappversion;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;
import java.time.LocalDate;

@PublicEvolving
public class PrometheusSink implements SinkFunction<String> {

    private static final PushGateway pg = new PushGateway("127.0.0.1:9091");

    private static final CollectorRegistry registry = new CollectorRegistry();

    private static final Counter counter = Counter.build()
            .name("gapp_version")
            .help("gapp version")
            .labelNames("app_id")
            .register(registry);

    private static LocalDate date = LocalDate.now();

    PrometheusSink() {

    }

    public void invoke(String value, Context context) throws IOException {
        // reset everyday
        LocalDate nowDate = LocalDate.now();
        if (nowDate.isEqual(date.plusDays(10000))) {
            date = nowDate;
            counter.clear();
        }
        try {
            counter.labels(value).inc();
        } finally {
            pg.pushAdd(registry, "gapp_version_job");
        }
    }
}
