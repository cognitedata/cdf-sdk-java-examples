package com.cognite.examples;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.RawRow;
import com.google.common.base.Preconditions;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Optional;

public class Beam {
    private static Logger LOG = LoggerFactory.getLogger(Beam.class);

    /*
    CDF project config. From config file / env variables.
     */
    private static final String cdfHost =
            ConfigProvider.getConfig().getValue("cognite.host", String.class);
    private static final String cdfProject =
            ConfigProvider.getConfig().getValue("cognite.project", String.class);
    private static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("cognite.apiKey", String.class);
    private static final Optional<String> clientId =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientId", String.class);
    private static final Optional<String> clientSecret =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientSecret", String.class);
    private static final Optional<String> aadTenantId =
            ConfigProvider.getConfig().getOptionalValue("cognite.azureADTenantId", String.class);
    private static final String[] authScopes =
            ConfigProvider.getConfig().getValue("cognite.scopes", String[].class);

    /*
    CDF.Raw source table configuration. From config file / env variables.
     */
    private static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    private static final String rawTable =
            ConfigProvider.getConfig().getValue("source.table", String.class);

    /*
    CDF data target configuration. From config file / env variables.
     */
    private static final Optional<String> targetDataSetExtId =
            ConfigProvider.getConfig().getOptionalValue("target.dataSetExternalId", String.class);
    private static final Optional<String> extractionPipelineExtId =
            ConfigProvider.getConfig().getOptionalValue("target.extractionPipelineExternalId", String.class);

    /*
    Pipeline configuration
     */
    private static final String extIdPrefix = "source-name:";

    /*
    Metrics target configuration. From config file / env variables.
     */
    private static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    private static final String metricsJobName =
            ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    private static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);

    /*
    Metrics section. Define the metrics to expose.
     */
    private static final CollectorRegistry collectorRegistry = new CollectorRegistry();
    private static final Gauge jobDurationSeconds = Gauge.build()
            .name("job_duration_seconds").help("Job duration in seconds").register(collectorRegistry);
    private static final Gauge jobStartTimeStamp = Gauge.build()
            .name("job_start_timestamp").help("Job start timestamp").register(collectorRegistry);
    private static final Gauge errorGauge = Gauge.build()
            .name("job_errors").help("Total job errors").register(collectorRegistry);
    private static final Gauge noElementsGauge = Gauge.build()
            .name("job_no_elements_processed").help("Number of processed elements").register(collectorRegistry);
    private static final Gauge noElementsContextualizedGauge = Gauge.build()
            .name("job_no_elements_contextualized").help("Number of contextualized elements").register(collectorRegistry);

    // global data structures

    /**
     * Concept #2: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line. This DoFn tokenizes lines of text into individual words; we pass it to
     * a ParDo in the pipeline.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private final Counter emptyLines = Metrics.counter(ExtractWordsFn.class, "emptyLines");
        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void processElement(@Element String element, OutputReceiver<String> receiver) {
            lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                emptyLines.inc();
            }

            // Split the line into words.
            String[] words = element.split("[^\\p{L}]+", 0);

            // Output each word encountered into the output PCollection.
            for (String word : words) {
                if (!word.isEmpty()) {
                    receiver.output(word);
                }
            }
        }
    }

    /** A SimpleFunction that converts a Word and Count into a printable string. */
    public static class FormatAsTextFn extends SimpleFunction<KV<String, Long>, String> {
        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

    /**
     * A PTransform that converts a PCollection containing lines of text into a PCollection of
     * formatted word counts.
     *
     * <p>Concept #3: This is a custom composite transform that bundles two transforms (ParDo and
     * Count) as a reusable PTransform subclass. Using composite transforms allows for easy reuse,
     * modular testing, and an improved monitoring experience.
     */
    public static class CountWords
            extends PTransform<PCollection<String>, PCollection<KV<String, Long>>> {
        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {

            // Convert lines of text into individual words.
            PCollection<String> words = lines.apply(ParDo.of(new ExtractWordsFn()));

            // Count the number of times each word occurs.
            PCollection<KV<String, Long>> wordCounts = words.apply(Count.perElement());

            return wordCounts;
        }
    }

    /*
    The main logic to execute.
     */
    static PipelineResult runWordCount(PipelineOptions options) throws Exception {
        Pipeline p = Pipeline.create(options);

        /*
        Read the Raw DB table into a PCollection
         */
        PCollection<RawRow> rawRowPCollection = p
                .apply("Read raw table", CogniteIO.readRawRow()
                        .withProjectConfig(getProjectConfig())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier("my-beam-app"))
                        .withRequestParameters(RequestParameters.create()
                                .withDbName(rawDb)
                                .withTableName(rawTable)));

        // Concepts #2 and #3: Our pipeline applies the composite CountWords transform, and passes the
        // static FormatAsTextFn() to the ParDo transform.
        p.apply("ReadLines", TextIO.read().from(sourceFile))
                //.apply("filter", Filter.by(element -> ThreadLocalRandom.current().nextDouble() < 0.01))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                /*
                .apply("Log counts", MapElements.into(TypeDescriptors.strings())
                        .via(entry -> {
                            LOG.info("Entry: {}", entry);
                            return entry;
                        }))

                 */
                .apply("WriteCounts", TextIO.write()
                        .to(targetFile)
                        .withoutSharding());

        return p.run();
    }

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        PipelineOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

        PipelineResult result = runWordCount(options);
        LOG.info("Started pipeline");
        result.waitUntilFinish();

        LOG.info("Pipeline finished with status: {}", result.getState().toString());
        long totalMemMb = Runtime.getRuntime().totalMemory() / (1024 * 1024);
        long freeMemMb = Runtime.getRuntime().freeMemory() / (1024 * 1024);
        long usedMemMb = totalMemMb - freeMemMb;
        String logMessage = String.format("----------------------- memory stats --------------------- %n"
                + "Total memory: %d MB %n"
                + "Used memory: %d MB %n"
                + "Free memory: %d MB", totalMemMb, usedMemMb, freeMemMb);
        LOG.info(logMessage);
    }


    /*
    Return the ProjectConfig.
     */
    private static ProjectConfig getProjectConfig() throws Exception {
        if (clientId.isPresent() && clientSecret.isPresent() && aadTenantId.isPresent()) {
            return ProjectConfig.create()
                    .withProject(cdfProject)
                    .withHost(cdfHost)
                    .withClientId(clientId.get())
                    .withClientSecret(clientSecret.get())
                    .withTokenUrl(TokenUrl.generateAzureAdURL(aadTenantId.get()).toString());

        } else if (apiKey.isPresent()) {
            return ProjectConfig.create()
                    .withProject(cdfProject)
                    .withHost(cdfHost)
                    .withApiKey(apiKey.get());
        } else {
            String message = "Unable to set up the Project Config. No valid authentication configuration.";
            LOG.error(message);
            throw new Exception(message);
        }
    }
}
