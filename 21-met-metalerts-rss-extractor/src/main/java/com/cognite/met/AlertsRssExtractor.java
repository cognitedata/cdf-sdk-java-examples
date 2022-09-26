package com.cognite.met;

import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.ExtractionPipelineRun;
import com.cognite.client.dto.RawRow;
import com.google.protobuf.Struct;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class AlertsRssExtractor {
    private static Logger LOG = LoggerFactory.getLogger(AlertsRssExtractor.class);

    /*
    CDF project config. From config file / env variables.
     */
    private static final String cdfHost =
            ConfigProvider.getConfig().getValue("cognite.host", String.class);
    private static final String cdfProject =
            ConfigProvider.getConfig().getValue("cognite.project", String.class);
    private static final String clientId =
            ConfigProvider.getConfig().getValue("cognite.clientId", String.class);
    private static final String clientSecret =
            ConfigProvider.getConfig().getValue("cognite.clientSecret", String.class);
    private static final String aadTenantId =
            ConfigProvider.getConfig().getValue("cognite.azureADTenantId", String.class);
    private static final String[] authScopes =
            ConfigProvider.getConfig().getValue("cognite.scopes", String[].class);

    /*
    Source RSS config
     */
    private static final String sourceUri = ConfigProvider.getConfig().getValue("source.uri", String.class);

    /*
    CDF.Raw target table configuration. From config file / env variables.
     */
    private static final String rawDb = ConfigProvider.getConfig().getValue("target.rawDb", String.class);
    private static final String rawTable =
            ConfigProvider.getConfig().getValue("target.rawTable", String.class);
    private static final Optional<String> extractionPipelineExtId =
            ConfigProvider.getConfig().getOptionalValue("target.extractionPipelineExternalId", String.class);

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
    private static final Gauge errorGauge = Gauge.build()
            .name("job_errors").help("Total job errors").register(collectorRegistry);
    private static final Gauge noElementsGauge = Gauge.build()
            .name("job_no_elements_processed").help("Number of processed elements").register(collectorRegistry);

    // global data structures
    private static CogniteClient cogniteClient;

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        boolean jobFailed = false;
        try {
            // Execute the main logic
            run();

            if (extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.SUCCESS,
                        String.format("Upserted %d RSS items to CDF Raw.",
                                noElementsGauge.get()));
            }
        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
            jobFailed = true;
            if (extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.FAILURE,
                        String.format("Job failed: %s", e.getMessage()));
            }
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
        LOG.info("Starting Met Alerts RSS extractor...");

        // Prepare the job duration metrics
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();

        LOG.info("Sending request to source uri: {}", sourceUri);

        // Set up the rss reader
        RssReader reader = new RssReader();
        List<Item> rssItems = reader.read(sourceUri).collect(Collectors.toList());
        LOG.info("Received {} RSS items", rssItems.size());

        // Parse the rss items to raw rows
        List<RawRow> rawRows = new ArrayList<>();
        for (Item item : rssItems) {
            rawRows.add(parseRawRow(item));
        }

        LOG.info("Writing {} rows to CDF {}.{}",
                rawRows.size(),
                rawDb,
                rawTable);
        getCogniteClient().raw().rows().upsert(rawRows);
        noElementsGauge.inc(rawRows.size());

        // All done
        jobDurationTimer.setDuration();
        LOG.info("Finished processing {} rss items. Duration {}",
                noElementsGauge.get(),
                Duration.ofSeconds((long) jobDurationSeconds.get()));

        // The job completion metric is only added to the registry after job success,
        // so that a previous success in the Pushgateway isn't overwritten on failure.
        Gauge jobCompletionTimeStamp = Gauge.build()
                .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
        jobCompletionTimeStamp.setToCurrentTime();
    }

    /*
    Parse the RSS item to a raw row.
     */
    private static RawRow parseRawRow(Item rssItem) {
        final String loggingPrefix = "parseRawRow() - ";

        final String titleKey = "title";
        final String descriptionKey = "description";
        final String linkKey = "link";
        final String authorKey = "author";
        final String categoryKey = "category";
        final String pubDateStringKey = "publishDateString";

        RawRow.Builder rowBuilder = RawRow.newBuilder()
                .setDbName(rawDb)
                .setTableName(rawTable)
                .setKey(rssItem.getGuid().get());

        Struct.Builder structBuilder = Struct.newBuilder();

        // parse the various expected field with check
        rssItem.getTitle().ifPresentOrElse(
                title -> structBuilder.putFields(titleKey, Values.of(title)),
                () -> LOG.warn(loggingPrefix + "No title for item {}", rssItem)
        );
        rssItem.getDescription().ifPresentOrElse(
                desc -> structBuilder.putFields(descriptionKey, Values.of(desc)),
                () -> LOG.warn(loggingPrefix + "No description for item {}", rssItem)
        );
        rssItem.getLink().ifPresentOrElse(
                link -> structBuilder.putFields(linkKey, Values.of(link)),
                () -> LOG.warn(loggingPrefix + "No link for item {}", rssItem)
        );
        rssItem.getAuthor().ifPresentOrElse(
                author -> structBuilder.putFields(authorKey, Values.of(author)),
                () -> LOG.warn(loggingPrefix + "No author for item {}", rssItem)
        );
        rssItem.getCategory().ifPresentOrElse(
                category -> structBuilder.putFields(categoryKey, Values.of(category)),
                () -> LOG.warn(loggingPrefix + "No category for item {}", rssItem)
        );
        rssItem.getPubDate().ifPresentOrElse(
                pubDate -> structBuilder.putFields(pubDateStringKey, Values.of(pubDate)),
                () -> LOG.warn(loggingPrefix + "No publishing date for item {}", rssItem)
        );


        RawRow row = rowBuilder.setColumns(structBuilder).build();
        LOG.debug(loggingPrefix + "Parsed raw row: \n {}", row);
        return row;
    }


    /*
    Return the Cognite client.

    If the client isn't instantiated, it will be created according to the configured authentication options. After the
    initial instantiation, the client will be cached and reused.
     */
    private static CogniteClient getCogniteClient() throws Exception {
        if (null == cogniteClient) {
            // The client has not been instantiated yet
            cogniteClient = CogniteClient.ofClientCredentials(
                            clientId,
                            clientSecret,
                            TokenUrl.generateAzureAdURL(aadTenantId),
                            Arrays.asList(authScopes))
                    .withProject(cdfProject)
                    .withBaseUrl(cdfHost);
        }

        return cogniteClient;
    }

    /*
    Creates an extraction pipeline run and writes it to Cognite Data Fusion.
     */
    private static boolean writeExtractionPipelineRun(ExtractionPipelineRun.Status status, String message) {
        boolean writeSuccess = false;
        if (extractionPipelineExtId.isPresent()) {
            try {
                ExtractionPipelineRun pipelineRun = ExtractionPipelineRun.newBuilder()
                        .setExternalId(extractionPipelineExtId.get())
                        .setCreatedTime(Instant.now().toEpochMilli())
                        .setStatus(status)
                        .setMessage(message)
                        .build();

                LOG.info("Writing extraction pipeline run with status: {}", pipelineRun);
                getCogniteClient().extractionPipelines().runs().create(List.of(pipelineRun));
                writeSuccess = true;
            } catch (Exception e) {
                LOG.warn("Error when trying to create extraction pipeline run: {}", e.toString());
            }
        } else {
            LOG.warn("Extraction pipeline external id is not configured. Cannot create pipeline run.");
        }

        return writeSuccess;
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
