package com.cognite.sa.api;

import com.cognite.client.CogniteClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Instant;

/**
 * This class hosts the core logic of all service operations.
 */
@ApplicationScoped
public class CopyTableService {
    private static Logger LOG = LoggerFactory.getLogger(CopyTableService.class);

    @Inject
    CogniteClient client;

    // Defaults


    public CopyTableResponse copyTable(CopyTableRequest copyRequest) throws Exception {
        Instant startInstant = Instant.now();

        CopyTableResponse response = new CopyTableResponse();
        response.message = "good response";
        return response;
    }
}
