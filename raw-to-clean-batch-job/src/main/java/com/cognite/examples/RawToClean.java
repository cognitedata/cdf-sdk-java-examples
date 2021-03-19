package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.Event;
import com.cognite.client.dto.RawRow;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.protobuf.StringValue;
import com.google.protobuf.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class RawToClean {
    private static Logger LOG = LoggerFactory.getLogger(RawToClean.class);
    private final static String baseURL = "https://api.cognitedata.com";

    public static void main(String[] args) throws Exception {
        Instant startInstant = Instant.now();
        int countRows = 0;

        // Get the source table via env variables
        String dbName = System.getenv("RAW_DB").split("\\.")[0];
        String dbTable = System.getenv("RAW_DB").split("\\.")[1];

        CogniteClient client = getClient();

        // Set up the reader for the raw table
        Iterator<List<RawRow>> iterator = client.raw().rows().list(dbName, dbTable);

        // Iterate through all rows in batches and write to clean. This will effectively "stream" through
        // the data so that we have constant memory usage no matter how large the data set is.
        while (iterator.hasNext()) {
            List<Event> events = iterator.next().stream()
                    .map(row -> {
                        // Collect all columns into the metadata bucket of the event
                        Map<String, String> metadata = row.getColumns().getFieldsMap().entrySet().stream()
                                .collect(Collectors.toMap((Map.Entry<String, Value> entry) -> entry.getKey(),
                                        entry -> entry.getValue().getStringValue()));

                        // Build the event object
                        return Event.newBuilder()
                                .setExternalId(StringValue.of(row.getTableName() + row.getKey()))
                                .setDescription(StringValue.of(row.getColumns().getFieldsOrThrow("description").getStringValue()))
                                .putAllMetadata(metadata)
                                .build();
                    })
                    .collect(Collectors.toList());

            client.events().upsert(events);
            countRows += events.size();
        }

        LOG.info("Finished processing {} rows from raw. Duration {}",
                countRows,
                Duration.between(startInstant, Instant.now()));
    }

    /*
    Instantiate the cognite client based on an api key hosted in GCP Secret Manager (key vault).
     */
    private static CogniteClient getClient() throws Exception {
        // Instantiate the client
        LOG.info("Start instantiate the Cognite Client.");

        LOG.info("API key is hosted in Secret Manager.");
        String projectId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[0];
        String secretId = System.getenv("CDF_API_KEY_SECRET_MANAGER").split("\\.")[1];
        return CogniteClient.ofKey(getGcpSecret(projectId, secretId, "latest"))
                .withBaseUrl(baseURL);
    }

    /*
    Read secrets from GCP Secret Manager.
    Since we are using workload identity on GKE, we have to take into account that the identity metadata
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
