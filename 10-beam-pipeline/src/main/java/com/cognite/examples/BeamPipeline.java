package com.cognite.examples;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.CogniteClient;
import com.cognite.client.config.ClientConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.config.UpsertMode;
import com.cognite.client.dto.*;
import com.cognite.client.util.ParseValue;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import com.google.protobuf.util.Structs;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.metrics.*;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.cognite.examples.BeamPipelineConfig.*;

public class BeamPipeline {
    private static Logger LOG = LoggerFactory.getLogger(BeamPipeline.class);

    /*
    Pipeline configuration
     */
    private static final String appIdentifier = "my-beam-app";
    private static final String extIdPrefix = "source-name:";
    private static final String configKeyDataSetId = "dataSetId";


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
    private static CogniteClient cogniteClient;
    private static OptionalLong dataSetIntId;

    /**
     * Concept #1: You can make your pipeline assembly code less verbose by defining your DoFns
     * statically out-of-line.
     *
     * This DoFn parses the Raw rows into CDF Events.
     */
    static class ParseRowToEventFn extends DoFn<RawRow, Event> {
        final String loggingPrefix = "ParseRowToEventFn() - ";

        /*
        Configuration section. Defines key (raw) columns and values for the parsing and transform logic.
         */
        // Key columns
        // These raw columns map to the event schema fields
        final String extIdKey = "RawExtIdColumn";
        final String descriptionKey = "RawDescriptionColumn";
        final String startDateTimeKey = "RawStartDateTimeColumn";
        final String endDataTimeKey = "RawEndDateTimeColumn";

        // Fixed values
        // Hardcoded values to add to the event schema fields
        final String typeValue = "event-type";
        final String subtypeValue = "event-subtype";
        final String sourceValue = "data-source-name";

        // Include / exclude columns
        // For filtering the entries to the metadata bucket
        final String excludeColumnPrefix = "exclude__";
        List<String> excludeColumns = List.of("exclude-column-a", "exclude-column-b", "exclude-column-c");

        // Metrics
        private final Counter noElements = Metrics.counter(ParseRowToEventFn.class, "noElements");

        // Side inputs
        final PCollectionView<Struct> configView;

        public ParseRowToEventFn(final PCollectionView<Struct> configView) {
            this.configView = configView;
        }

        @ProcessElement
        public void processElement(@Element RawRow element,
                                   OutputReceiver<Event> output,
                                   ProcessContext context) throws Exception {
            noElements.inc();
            Struct configStruct = context.sideInput(configView);
            Event.Builder eventBuilder = Event.newBuilder();
            Map<String, Value> columnsMap = element.getColumns().getFieldsMap();

            // Add the mandatory fields
            // If a mandatory field is missing, you should flag it and handle that record specifically. Either by failing
            // the entire job, or putting the failed records in a "dead letter queue".
            if (columnsMap.containsKey(extIdKey) && columnsMap.get(extIdKey).hasStringValue()) {
                eventBuilder.setExternalId(extIdPrefix + columnsMap.get(extIdKey).getStringValue());
            } else {
                String message = String.format(loggingPrefix + "Could not parse field [%s].",
                        extIdKey);
                LOG.error(message);
                throw new Exception(message);
            }
            if (columnsMap.containsKey(descriptionKey) && columnsMap.get(descriptionKey).hasStringValue()) {
                eventBuilder.setDescription(columnsMap.get(descriptionKey).getStringValue());
            } else {
                String message = String.format(loggingPrefix + "Could not parse field [%s].",
                        descriptionKey);
                LOG.error(message);
                throw new Exception(message);
            }

            // Add optional fields. If an optional field is missing, no need to take any action (usually)
            if (columnsMap.containsKey(startDateTimeKey) && columnsMap.get(startDateTimeKey).hasNumberValue()) {
                eventBuilder.setStartTime((long) columnsMap.get(startDateTimeKey).getNumberValue());
            }
            if (columnsMap.containsKey(endDataTimeKey) && columnsMap.get(endDataTimeKey).hasNumberValue()) {
                eventBuilder.setEndTime((long) columnsMap.get(endDataTimeKey).getNumberValue());
            }

            // Add fixed values
            eventBuilder
                    .setSource(sourceValue)
                    .setType(typeValue)
                    .setSubtype(subtypeValue);

            // Add fields to metadata based on the exclusion filters
            Map<String, String> metadata = columnsMap.entrySet().stream()
                    .filter(entry -> !entry.getKey().startsWith(excludeColumnPrefix))
                    .filter(entry -> !excludeColumns.contains(entry.getKey()))
                    .collect(Collectors.toMap(Map.Entry::getKey, entry -> ParseValue.parseString(entry.getValue())));

            // Add basic lineage info
            metadata.put("dataSource",
                    String.format("CDF Raw: %s.%s.%s", element.getDbName(), element.getTableName(), element.getKey()));

            // Don't forget to add the metadata to the event object
            eventBuilder.putAllMetadata(metadata);

            // If a target dataset has been configured, add it to the event object
            if (configStruct.getFieldsOrDefault(configKeyDataSetId, Values.ofNull()).hasStringValue()) {
                long dataSetId = Long.parseLong(configStruct.getFieldsOrThrow(configKeyDataSetId).getStringValue());
                eventBuilder.setDataSetId(dataSetId);
            }

            // Build the event object
            output.output(eventBuilder.build());
        }
    }

    static class ContextualizeEventFn extends DoFn<Event, Event> {
        final String loggingPrefix = "ContextualizeEventFn() - ";
        // Contextualization configuration
        final String assetReferenceKey = "RawAssetNameReferenceColumn";

        // Metrics
        private final Counter matchElementCounter = Metrics.counter(ContextualizeEventFn.class, "matchedElement");

        // Side inputs
        final PCollectionView<Map<String, Asset>> assetMapView;

        public ContextualizeEventFn(PCollectionView<Map<String, Asset>> assetMapView) {
            this.assetMapView = assetMapView;
        }

        @ProcessElement
        public void processElement(@Element Event element,
                                   OutputReceiver<Event> output,
                                   ProcessContext context) {
            Map<String, Asset> assetMap = context.sideInput(assetMapView);

            /*
            Contextualization.
            - Do a pure name-based, exact match asset lookup.
            - Log a successful contextualization operation as a metric.
             */
            Map<String, String> metadata = element.getMetadataMap();
            if (metadata.containsKey(assetReferenceKey) && assetMap.containsKey(metadata.get(assetReferenceKey))) {
                output.output(element.toBuilder()
                        .addAssetIds(assetMap.get(metadata.get(assetReferenceKey)).getId())
                        .build());

                matchElementCounter.inc();
            } else {
                LOG.warn(loggingPrefix + "Not able to link event to asset. Source input for column {}: {}",
                        assetReferenceKey,
                        metadata.getOrDefault(assetReferenceKey, "null"));
                output.output(element);
            }
        }


    }

    /*
    The main logic to execute.
     */
    static PipelineResult runBeamPipeline(PipelineOptions options) throws Exception {
        Pipeline p = Pipeline.create(options);

        /*
        Build the config object to be offered to all transforms.

        The config must be created as a data object at pipeline build time so it
        can be serialized and sent to all workers executing the pipeline.
         */
        Struct configStruct = Structs.of(
                configKeyDataSetId, com.google.protobuf.util.Values.ofNull()
        );
        if (getDataSetIntId().isPresent()) {
            configStruct = configStruct.toBuilder()
                    .putFields(configKeyDataSetId, Values.of(String.valueOf(getDataSetIntId().getAsLong())))
                    .build();
        }

        /*
        Map the config object to a view so it can be accessed by transforms.
         */
        PCollectionView<Struct> configView = p
                .apply("Build Config", Create.of(configStruct))
                .apply("Log config", MapElements.into(TypeDescriptor.of(Struct.class))
                        .via(struct -> {
                            LOG.info("Config object: \n{}", struct.toString());
                            return struct;
                        }))
                .apply("To view", View.asSingleton());

        /*
        Reads the existing assets from CDF--will be used for contextualization:
        1) Read the full collection of assets from CDF.
        2) Minimize the size of the asset lookup. For performance optimization.
        3) Group per key (to ensure a unique asset per lookup key). In case of multiple candidates, use the most
        recently updated asset.
        4) Publish the assets to a view so they can be used for memory-based lookup.
         */
        PCollectionView<Map<String, Asset>> assetsMap = p
                .apply("Read assets", CogniteIO.readAssets()
                        .withProjectConfig(getProjectConfig())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)))
                .apply("Minimize", MapElements.into(TypeDescriptor.of(Asset.class))
                        .via((Asset input) ->
                                input.toBuilder()
                                        .clearMetadata()
                                        .clearSource()
                                        .build()))
                .apply("Add key (name)", WithKeys.of(Asset::getName))
                .setCoder(KvCoder.of(StringUtf8Coder.of(), ProtoCoder.of(Asset.class)))
                .apply("Select newest asset per key", Max.perKey((Comparator<Asset> & Serializable) (left, right) ->
                        Long.compare(left.getLastUpdatedTime(), right.getLastUpdatedTime())))
                .apply("To map view", View.asMap());

        /*
        The main logic.

        - Read the Raw DB table
        - Parse rows into events
        - Contextualize the events
        - Write to CDF
         */
        PCollection<Event> result = p
                .apply("Read raw table", CogniteIO.readRawRow()
                        .withProjectConfig(getProjectConfig())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier))
                        .withRequestParameters(RequestParameters.create()
                                .withDbName(rawDb)
                                .withTableName(rawTable)))
                .apply("Parse row", ParDo.of(new ParseRowToEventFn(configView))
                        .withSideInputs(configView))
                //.apply("Contextualize event", ParDo.of(new ContextualizeEventFn(assetsMap))
                //        .withSideInputs(assetsMap))
                .apply("Write events", CogniteIO.writeEvents()
                        .withProjectConfig(getProjectConfig())
                        .withWriterConfig(WriterConfig.create()
                                .withAppIdentifier(appIdentifier)));

        return p.run();
    }

    /*
    The entry point of the code. It executes the main logic and push job metrics upon completion.
     */
    public static void main(String[] args) {
        boolean jobFailed = false;
        // Prepare the job start metrics
        Gauge.Timer jobDurationTimer = jobDurationSeconds.startTimer();
        jobStartTimeStamp.setToCurrentTime();

        try {
            // Read arguments and pass them to the pipeline as options
            PipelineOptions options =
                    PipelineOptionsFactory.fromArgs(args).withValidation().as(PipelineOptions.class);

            LOG.info("Starting pipeline...");
            PipelineResult result = runBeamPipeline(options);
            result.waitUntilFinish();

            LOG.info("Pipeline finished with status: {}", result.getState().toString());
            if (result.getState() != PipelineResult.State.DONE) {
                // The Beam pipeline did not complete successfully
                throw new Exception(String.format("Job failed. Beam pipeline completed with status: %s", result.getState().toString()));
            }

            LOG.info("Collect metrics from the Beam pipeline");
            Map<String, Gauge> gaugeMap = Map.of(
                    "noElements", noElementsGauge
            );
            MetricQueryResults metrics = result
                    .metrics()
                    .queryMetrics(MetricsFilter.builder()
                            .addNameFilter(MetricNameFilter.inNamespace("cognite"))
                            .build());

            LOG.info("The gaugeMap key set: {}", gaugeMap.keySet());

            LOG.info("Log counter metrics");
            for (MetricResult<Long> counter: metrics.getCounters()) {
                LOG.info(counter.getName().getName() + ":" + counter.getAttempted());
                if (gaugeMap.containsKey(counter.getName().getName())) {
                    LOG.info("Got a match on a counter. Will add to prom metrics");
                    gaugeMap.get(counter.getName().getName()).set(counter.getAttempted());
                }
            }

            LOG.info("Log distribution metrics");
            for (MetricResult<DistributionResult> distribution : metrics.getDistributions()) {
                LOG.info(distribution.getName().getName() + ":" + distribution.getAttempted().getMean());
            }

            // All done
            jobDurationTimer.setDuration();
            LOG.info("Finished processing {} rows from raw. Duration {}",
                    noElementsGauge.get(),
                    Duration.ofSeconds((long) jobDurationSeconds.get()));

            // The job completion metric is only added to the registry after job success,
            // so that a previous success in the Pushgateway isn't overwritten on failure.
            Gauge jobCompletionTimeStamp = Gauge.build()
                    .name("job_completion_timestamp").help("Job completion time stamp").register(collectorRegistry);
            jobCompletionTimeStamp.setToCurrentTime();

            // Report success status to the extraction pipeline
            if (BeamPipelineConfig.extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.SUCCESS,
                        String.format("Upserted %d events to CDF. %d events could be linked to assets.",
                                (int) noElementsGauge.get(),
                                (int) noElementsContextualizedGauge.get()));
            }

        } catch (Exception e) {
            LOG.error("Unrecoverable error. Will exit. {}", e.toString());
            errorGauge.inc();
            jobFailed = true;
            if (BeamPipelineConfig.extractionPipelineExtId.isPresent()) {
                writeExtractionPipelineRun(ExtractionPipelineRun.Status.FAILURE,
                        String.format("Job failed: %s", e.getMessage()));
            }
        } finally {
            if (BeamPipelineConfig.enableMetrics) {
                pushMetrics();
            }
            if (jobFailed) {
                System.exit(1); // container exit code for execution errors, etc.
            }
        }
    }


    /*
    Return the ProjectConfig.
     */
    private static ProjectConfig getProjectConfig() throws MalformedURLException {
        return ProjectConfig.create()
                .withProject(BeamPipelineConfig.cdfProject)
                .withHost(BeamPipelineConfig.cdfHost)
                .withClientId(BeamPipelineConfig.clientId)
                .withClientSecret(BeamPipelineConfig.clientSecret)
                .withTokenUrl(TokenUrl.generateAzureAdURL(BeamPipelineConfig.aadTenantId).toString());
    }

    /*
    Return the data set internal id.

    If the data set external id has been configured, this method will translate this to the corresponding
    internal id.
     */
    private static OptionalLong getDataSetIntId() throws Exception {
        if (null == dataSetIntId) {
            if (BeamPipelineConfig.targetDataSetExtId.isPresent()) {
                // Get the data set id
                LOG.info("Looking up the data set external id: {}.",
                        BeamPipelineConfig.targetDataSetExtId.get());
                List<DataSet> dataSets = getCogniteClient().datasets()
                        .retrieve(ImmutableList.of(Item.newBuilder().setExternalId(BeamPipelineConfig.targetDataSetExtId.get()).build()));

                if (dataSets.size() != 1) {
                    // The provided data set external id cannot be found.
                    String message = String.format("The configured data set external id does not exist: %s", BeamPipelineConfig.targetDataSetExtId.get());
                    LOG.error(message);
                    throw new Exception(message);
                }
                dataSetIntId = OptionalLong.of(dataSets.get(0).getId());
            } else {
                dataSetIntId = OptionalLong.empty();
            }
        }

        return dataSetIntId;
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
                            TokenUrl.generateAzureAdURL(aadTenantId))
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
        if (BeamPipelineConfig.extractionPipelineExtId.isPresent()) {
            try {
                ExtractionPipelineRun pipelineRun = ExtractionPipelineRun.newBuilder()
                        .setExternalId(BeamPipelineConfig.extractionPipelineExtId.get())
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
        if (BeamPipelineConfig.pushGatewayUrl.isPresent()) {
            try {
                LOG.info("Pushing metrics to {}", BeamPipelineConfig.pushGatewayUrl);
                PushGateway pg = new PushGateway(new URL(BeamPipelineConfig.pushGatewayUrl.get())); //9091
                pg.pushAdd(collectorRegistry, BeamPipelineConfig.metricsJobName);
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
