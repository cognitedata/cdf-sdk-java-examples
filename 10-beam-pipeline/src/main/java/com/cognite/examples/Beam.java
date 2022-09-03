package com.cognite.examples;

import com.cognite.beam.io.CogniteIO;
import com.cognite.beam.io.RequestParameters;
import com.cognite.beam.io.config.ProjectConfig;
import com.cognite.beam.io.config.ReaderConfig;
import com.cognite.beam.io.config.WriterConfig;
import com.cognite.client.config.TokenUrl;
import com.cognite.client.dto.*;
import com.cognite.client.util.ParseValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.Values;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.eclipse.microprofile.config.ConfigProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
    private static final String appIdentifier = "my-beam-app";
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
        final PCollectionView<Map<String, Long>> dataSetsExtIdMapView;

        public ParseRowToEventFn(PCollectionView<Map<String, Long>> dataSetsExtIdMapView) {
            this.dataSetsExtIdMapView = dataSetsExtIdMapView;
        }

        @ProcessElement
        public void processElement(@Element RawRow element,
                                   OutputReceiver<Event> output,
                                   ProcessContext context) throws Exception {
            noElements.inc();
            Map<String, Long> dataSetsMap = context.sideInput(dataSetsExtIdMapView);
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
            if (targetDataSetExtId.isPresent() && dataSetsMap.containsKey(targetDataSetExtId.get())) {
                eventBuilder.setDataSetId(dataSetsMap.get(targetDataSetExtId.get()));
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
    static PipelineResult runWordCount(PipelineOptions options) throws Exception {
        Pipeline p = Pipeline.create(options);

        /*
        Read the CDF data sets and build an external id to internal id map.
         */
        PCollectionView<Map<String, Long>> dataSetsExtIdMap = p
                .apply("Read target data sets", CogniteIO.readDataSets()
                        .withProjectConfig(getProjectConfig())
                        .withReaderConfig(ReaderConfig.create()
                                .withAppIdentifier(appIdentifier)
                                .enableMetrics(false)))
                .apply("Select externalId + id", MapElements
                        .into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                        .via((DataSet dataSet) -> {
                            LOG.info("Dataset - id: {}, extId: {}, name: {}",
                                    dataSet.getId(),
                                    dataSet.getExternalId(),
                                    dataSet.getName());
                            return KV.of(dataSet.getExternalId(), dataSet.getId());
                        }))
                .apply("Max per key", Max.perKey())
                .apply("To map view", View.asMap());

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
                .apply("Parse row", ParDo.of(new ParseRowToEventFn(dataSetsExtIdMap))
                        .withSideInputs(dataSetsExtIdMap))
                .apply("Contextualize event", ParDo.of(new ContextualizeEventFn(assetsMap))
                        .withSideInputs(assetsMap))
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
                    .withTokenUrl(TokenUrl.generateAzureAdURL(aadTenantId.get()).toString())
                    .withAuthScopes(authScopes);

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