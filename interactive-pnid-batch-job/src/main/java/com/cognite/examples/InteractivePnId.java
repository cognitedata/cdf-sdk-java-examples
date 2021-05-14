package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.Request;
import com.cognite.client.dto.*;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.StringValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class InteractivePnId {
    private static Logger LOG = LoggerFactory.getLogger(InteractivePnId.class);
    private final static String baseURL = "https://api.cognitedata.com";
    private final static String fileNameSuffixRegEx = "\\.[a-zA-Z]{1,4}$";
    private static String appIdentifier = "my-interactive-pnid-pipeline";

    /* Pipeline config parameters */
    // filters to select the source P&IDs
    private final static String fileSourceDataSetExternalId = "dataset:d2-lci-files";
    private final static String fileSourceFilterMetadataKey = "DOCUMENT TYPE CODE";
    private final static String fileSourceFilterMetadataValue = "XB";

    // filters to select the assets to use as lookup values for the P&ID service
    private final static String assetDataSetExternalId = "dataset:aveva-net-assets";

    // the target data set for the interactive P&IDs
    private final static String fileTargetDataSetExternalId = "dataset:d2-interactive-pnids";

    //
    private final static String fileTechLocationMetadataKey = "document:abp_tech_location";

    private static CogniteClient client = null;

    public static void main(String[] args) throws Exception {
        Instant startInstant = Instant.now();
        int countFiles = 0;

        // Read the assets which we'll use as a basis for looking up entities in the P&ID
        LOG.info("Start downloading assets.");
        List<Asset> assetsResults = readAssets();
        LOG.info("Finished downloading {} assets. Duration: {}",
                assetsResults.size(),
                Duration.between(startInstant, Instant.now()));

        /*
        We need to construct the actual lookup entities. This is input to the
        P&ID service and guides how entities are detected in the P&ID.
        For example, if you want to take spelling variations into account, then this is
        the place you would add that configuration.
         */
        LOG.info("Start building detect entities struct.");
        List<Struct> matchToEntities = new ArrayList<>();
        for (Asset asset : assetsResults) {
            Struct matchTo = Struct.newBuilder()
                    .putFields("externalId", Value.newBuilder()
                            .setStringValue(asset.getExternalId().getValue())
                            .build())
                    .putFields("name", Value.newBuilder()
                            .setStringValue(asset.getName())
                            .build())
                    .putFields("resourceType", Value.newBuilder()
                            .setStringValue("Asset")
                            .build())
                    .build();

            matchToEntities.add(matchTo);
        }
        LOG.info("Finished building detect entities struct. Duration: {}",
                Duration.between(startInstant, Instant.now()));

        /*
        Build the list of files to run through the interactive P&ID process. The P&ID service has
        some restrictions on which file types it supports, so you should make sure to include
        only valid file types.

        In this case we'll filter on PDFs. Since some sources
         */
        LOG.info("Start detect and convert.");
        Item file = Item.newBuilder()
                .setExternalId("D2:ID:0903d6c1801112e8:pdf")
                .build();

        List<PnIDResponse> detectResults = client.experimental()
                .pnid()
                .detectAnnotationsPnID(List.<Item>of(file), entities, "name", true);

        LOG.info("Finished Start detect and convert.. Duration: {}",
                Duration.between(startInstant, Instant.now()));


        // Iterate through all rows in batches and write to clean. This will effectively "stream" through
        // the data so that we have constant memory usage no matter how large the data set is.
        while (iterator.hasNext()) {
            List<Event> events = iterator.next().stream()
                    .map(row -> {
                        // Collect all columns into the metadata bucket of the event
                        Map<String, String> metadata = row.getColumns().getFieldsMap().entrySet().stream()
                                .collect(Collectors.toMap((Map.Entry<String, Value> entry) -> entry.getKey(),
                                        entry -> entry.getValue().getStringValue()));

                        // Add basic lineage info
                        metadata.put("dataSource",
                                String.format("%s.%s.%s", row.getDbName(), row.getTableName(), row.getKey()));

                        // Build the event object
                        return Event.newBuilder()
                                .setExternalId(StringValue.of(row.getTableName() + row.getKey()))
                                .setDescription(StringValue.of(
                                        row.getColumns().getFieldsOrThrow("my-mandatory-field").getStringValue()))
                                .putAllMetadata(metadata)
                                .setDataSetId(dataSets.get(0).getId())
                                .build();
                    })
                    .collect(Collectors.toList());

            client.events().upsert(events);
            countRows += events.size();
        }

        LOG.info("Finished processing {} files. Duration {}",
                countFiles,
                Duration.between(startInstant, Instant.now()));
    }

    /*
    Instantiate the cognite client based on an api key hosted in GCP Secret Manager (key vault).
     */
    private static CogniteClient getClient() throws Exception {
        if (null == client) {
            // Instantiate the client
            LOG.info("Start instantiate the Cognite Client.");

            LOG.info("API key is hosted in Secret Manager.");
            String projectId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[0];
            String secretId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[1];

            client = CogniteClient.ofKey(getGcpSecret(projectId, secretId, "latest"))
                    .withBaseUrl(baseURL);
        }

        return client;
    }

    /*
    Read the assets collection and minimize the asset objects.
     */
    private static List<Asset> readAssets() throws Exception {
        List<Asset> assetResults = new ArrayList<>();

        // Read assets based on a list filter. The SDK client gives you an iterator back
        // that lets you "stream" batches of results.
        Iterator<List<Asset>> resultsIterator = getClient().assets().list(Request.create()
                .withFilterParameter("dataSetIds", List.of(
                        Map.of("externalId", assetDataSetExternalId))
                )
                .withFilterMetadataParameter("FACILITY", "ULA")
        );

        // Read the asset results, one batch at a time.
        resultsIterator.forEachRemaining(assets -> {
            for (Asset asset : assets) {
                // we break out the results batch and process each individual result.
                // In this case we want minimize the size of the asset collection
                // by removing the metadata bucket (we don't need all that metadata for
                // our processing.
                assetResults.add(asset.toBuilder().clearMetadata().build());
            }
        });

        return assetResults;
    }

    /*
    Read secrets from GCP Secret Manager.
    If we are using workload identity on GKE, we have to take into account that the identity metadata
    service for the pod may take a few seconds to initialize. Therefore the implicit call to get
    identity may fail if it happens at the very start of the pod. The workaround is to perform a
    retry.
     */
    private static String getGcpSecret(String projectId, String secretId, String secretVersion) throws IOException {
        int maxRetries = 3;
        boolean success = false;
        IOException exception = null;
        String loggingPrefix = "getGcpSecret - ";
        String returnValue = "";

        for (int i = 0; i <= maxRetries && !success; i++) {
            // Initialize client that will be used to send requests.
            try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
                SecretVersionName name = SecretVersionName.of(projectId,
                        secretId, secretVersion);

                // Access the secret version.
                AccessSecretVersionRequest request =
                        AccessSecretVersionRequest.newBuilder().setName(name.toString()).build();
                AccessSecretVersionResponse response = client.accessSecretVersion(request);
                LOG.info(loggingPrefix + "Successfully read secret from GCP Secret Manager.");

                returnValue = response.getPayload().getData().toStringUtf8();
                success = true;
            } catch (IOException e) {
                String errorMessage = "Could not read secret from GCP secret manager. Will retry... " + e.getMessage();
                LOG.warn(errorMessage);
                exception = e;
            }
            if (!success) {
                // Didn't succeed in reading the secret. Pause the thread to wait for the metadata service to start.
                try {
                    Thread.sleep(1000l);
                } catch (Exception e) {
                    LOG.warn("Not able to pause thread: " + e);
                }
            }
        }

        if (!success) {
            // We didn't manage to read the secret
            LOG.error("Could not read secret from GCP secret manager: {}", exception.toString());
            throw exception;
        }

        return returnValue;
    }
}
