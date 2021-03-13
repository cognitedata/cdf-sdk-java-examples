package com.cognite.sa.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.time.Instant;

/**
 * Class that defines the HTTP endpoints for the copy table resource / functionality.
 */
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
@Path("copy")
public class CopyTableResource {
    private static final Logger LOG = LoggerFactory.getLogger(CopyTableResource.class);

    @POST
    public CopyTableResponse postCopy(CopyTableRequest copyRequest) {
        Instant startInstant = Instant.now();
        if (copyRequest == null) {
            throw new BadRequestException();
        }
        LOG.info("Received request: {}", copyRequest);
        if (copyRequest.sourceDb == null || copyRequest.sourceTable == null || copyRequest.targetDb == null
                || copyRequest.targetTable == null) {
            String message = "Request does not contain the required fields.";
            LOG.warn(message);
            throw new BadRequestException(message);
        }

        CopyTableResponse response = new CopyTableResponse();
        response.message = "good response";
        return response;
    }

    @GET
    public CopyTableResponse getCopy() {
        CopyTableResponse response = new CopyTableResponse();
        response.message = "good response";
        return response;
    }
}
