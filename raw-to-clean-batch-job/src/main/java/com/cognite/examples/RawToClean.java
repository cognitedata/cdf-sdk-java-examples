package com.cognite.examples;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.RawRow;
import com.google.cloud.secretmanager.v1.AccessSecretVersionRequest;
import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

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

        Iterator<List<RawRow>> iterator = client.raw().rows().list(dbName, dbTable);
        while (iterator.hasNext()) {

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
