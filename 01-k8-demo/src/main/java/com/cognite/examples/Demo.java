package com.cognite.examples;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Optional;


public class Demo {
    private static Logger LOG = LoggerFactory.getLogger(Demo.class);

    /*
    Configuration section. The configuration values are read from the following locations (in order of precedence):
    1. Environment variables
    2. Configuration file at /config/config.yaml
    3. The default config file at ./resources/META-INF/microprofile-config.yaml (packaged with the code)

    A configuration variable "foo.bar" resolves to the following input:
    - Environment variable named "foo_bar" (dot "." is replaced by underscore "_")
    - Config file yaml entry: "
    metrics:
        bar: "the-value"
    "
     */
    // Metrics configs. From config file / env variables
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName = ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);


    /*
    Metrics section. Define the metrics to expose.
     */
    static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    static final Gauge errorGauge = Gauge.build()
            .name("job_errors").help("Total job errors").register(collectorRegistry);

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        boolean jobFailed = false;
        try {
            // Execute the main logic
            run();

        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
            jobFailed = true;
        } finally {
            if (enableMetrics) {
                pushMetrics();
            }
            if (jobFailed) {
                System.exit(1); // container exit code for execution errors, etc.
            }
        }
    }

    /*
    The main logic to execute.
     */
    private static void run() throws Exception {
        LOG.info("Starting container...");
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();

        LOG.info("Starting some work...");
        Thread.sleep(3000);

        LOG.info("Finished work");
        jobDurationTimer.setDuration();

        // The job completion metric is only added to the registry after job success,
        // so that a previous success in the Pushgateway isn't overwritten on failure.
        Gauge jobCompletionTimeStamp = Gauge.build()
                .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
        jobCompletionTimeStamp.setToCurrentTime();
    }

    /*
    Push the current metrics to the push gateway.
     */
    private static boolean pushMetrics() {
        boolean isSuccess = false;
        if (pushGatewayUrl.isPresent()) {
            try {
                LOG.info("Pushing metrics to {}", pushGatewayUrl);
                PushGateway pg = new PushGateway(new URL(pushGatewayUrl.get())); //9091
                pg.pushAdd(collectorRegistry, metricsJobName);
                isSuccess = true;
            } catch (Exception e) {
                LOG.warn("Error when trying to push metrics: {}", e.toString());
            }
        } else {
            LOG.warn("No metrics push gateway configured. Cannot push the metrics.");
        }

        return isSuccess;
    }
}
