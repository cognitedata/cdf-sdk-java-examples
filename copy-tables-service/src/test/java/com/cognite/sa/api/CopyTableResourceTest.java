package com.cognite.sa.api;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.is;

@QuarkusTest
public class CopyTableResourceTest {

    @Disabled
    @Test
    public void testHelloEndpoint() {
        given()
          .when().get("/api/copy")
          .then()
             .statusCode(200)
             .body(is("The service is alive--but no copy operation has been performed."));
    }

}