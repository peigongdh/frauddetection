package prometheustest;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.flink.annotation.Public;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.IOException;

@Public
public class PrometheusSink<T> implements SinkFunction<T> {

    public void invoke(T value) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        Gauge duration = Gauge.build()
                .name("my_batch_job_duration_seconds").help("Duration of my batch job in seconds.").register(registry);
        Gauge.Timer durationTimer = duration.startTimer();
        try {
            // Your code here.

            // This is only added to the registry after success,
            // so that a previous success in the Pushgateway isn't overwritten on failure.
            Gauge lastSuccess = Gauge.build()
                    .name("my_batch_job_last_success").help("Last time my batch job succeeded, in unixtime.").register(registry);
            lastSuccess.setToCurrentTime();
        } finally {
            durationTimer.setDuration();
            PushGateway pg = new PushGateway("127.0.0.1:9091");
            pg.pushAdd(registry, "my_batch_job");
        }
    }
}
