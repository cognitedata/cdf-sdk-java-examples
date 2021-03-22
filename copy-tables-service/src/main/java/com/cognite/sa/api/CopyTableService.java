package com.cognite.sa.api;

import com.cognite.client.CogniteClient;
import com.cognite.client.dto.RawRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class hosts the core logic of all service operations.
 */
@ApplicationScoped
public class CopyTableService {
    private static Logger LOG = LoggerFactory.getLogger(CopyTableService.class);

    //@Inject
    //CogniteClient client;

    @Inject
    SharedResources sharedResources;

    // Defaults


    public CopyTableResponse copyTable(CopyTableRequest copyRequest) throws Exception {
        Instant startInstant = Instant.now();
        int countRows = 0;
        CogniteClient client = sharedResources.getClient();
        Iterator<List<RawRow>> iterator = client.raw().rows().list(copyRequest.sourceDb, copyRequest.sourceTable);
        while (iterator.hasNext()) {
            List<RawRow> rows = iterator.next().stream()
                    .map(rawRow -> rawRow.toBuilder()
                            .setDbName(copyRequest.targetDb)
                            .setTableName(copyRequest.targetTable)
                            .build())
                    .collect(Collectors.toList());
            client.raw().rows().upsert(rows);
            countRows += rows.size();
        }

        CopyTableResponse response = new CopyTableResponse();
        response.message = String.format("Copied %d rows from %s to %s with a duration of %s",
                countRows,
                copyRequest.sourceDb + "." + copyRequest.sourceTable,
                copyRequest.targetDb + "." + copyRequest.targetTable,
                Duration.between(startInstant, Instant.now()).toString());
        return response;
    }
}
