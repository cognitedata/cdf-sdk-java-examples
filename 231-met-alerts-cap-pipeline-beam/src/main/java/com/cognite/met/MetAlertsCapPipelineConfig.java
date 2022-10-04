package com.cognite.met;

import org.eclipse.microprofile.config.ConfigProvider;

import java.util.Optional;

public class MetAlertsCapPipelineConfig {
    /*
    CDF project config. From config file / env variables.
     */
    public static final String cdfHost =
            ConfigProvider.getConfig().getValue("cognite.host", String.class);
    public static final String cdfProject =
            ConfigProvider.getConfig().getValue("cognite.project", String.class);
    public static final Optional<String> apiKey =
            ConfigProvider.getConfig().getOptionalValue("cognite.apiKey", String.class);
    public static final Optional<String> clientId =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientId", String.class);
    public static final Optional<String> clientSecret =
            ConfigProvider.getConfig().getOptionalValue("cognite.clientSecret", String.class);
    public static final Optional<String> aadTenantId =
            ConfigProvider.getConfig().getOptionalValue("cognite.azureADTenantId", String.class);

    /*
    CDF.Raw source table configuration. From config file / env variables.
     */
    public static final String rawDb = ConfigProvider.getConfig().getValue("source.rawDb", String.class);
    public static final String rawTable =
            ConfigProvider.getConfig().getValue("source.table", String.class);

    /*
    CDF data target configuration. From config file / env variables.
     */
    public static final Optional<String> targetDataSetExtId =
            ConfigProvider.getConfig().getOptionalValue("target.dataSetExternalId", String.class);
    public static final Optional<String> extractionPipelineExtId =
            ConfigProvider.getConfig().getOptionalValue("target.extractionPipelineExternalId", String.class);

    /*
    Metrics target configuration. From config file / env variables.
     */
    public static final boolean enableMetrics =
            ConfigProvider.getConfig().getValue("metrics.enable", Boolean.class);
    public static final String metricsJobName =
            ConfigProvider.getConfig().getValue("metrics.jobName", String.class);
    public static final Optional<String> pushGatewayUrl =
            ConfigProvider.getConfig().getOptionalValue("metrics.pushGateway.url", String.class);
}
