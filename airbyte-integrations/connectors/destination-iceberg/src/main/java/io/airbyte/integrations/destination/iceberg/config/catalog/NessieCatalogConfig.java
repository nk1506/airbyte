/*
 * Copyright (c) 2023 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.iceberg.config.catalog;

import static io.airbyte.integrations.destination.iceberg.IcebergConstants.CATALOG_NAME;
import static io.airbyte.integrations.destination.iceberg.IcebergConstants.NESSIE_CATALOG_AUTHENTICATION_TYPE_KEY;
import static io.airbyte.integrations.destination.iceberg.IcebergConstants.NESSIE_CATALOG_REFERENCE_KEY;
import static io.airbyte.integrations.destination.iceberg.IcebergConstants.NESSIE_CATALOG_TOKEN_CONFIG_KEY;
import static io.airbyte.integrations.destination.iceberg.IcebergConstants.NESSIE_CATALOG_URI_CONFIG_KEY;
import static io.airbyte.integrations.destination.iceberg.IcebergConstants.NESSIE_CLIENT_API_VERSION_KEY;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_AUTH_TOKEN;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.nessie.NessieCatalog;
import org.jetbrains.annotations.NotNull;

@Data
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = false)
public class NessieCatalogConfig
    extends IcebergCatalogConfig {

  private final String uri;
  private final String ref;
  private final String authenticationType;
  private final String nessieApiVersion;
  private final String token;
  private static final String[] SUPPORTED_AUTH_TYPES = {"NONE", "BEARER"};

  public NessieCatalogConfig(@NotNull JsonNode catalogConfig) {
    Preconditions.checkArgument(null != catalogConfig.get(NESSIE_CATALOG_URI_CONFIG_KEY), "%s is required", NESSIE_CATALOG_URI_CONFIG_KEY);
    JsonNode tokenNode = catalogConfig.get(NESSIE_CATALOG_TOKEN_CONFIG_KEY);
    JsonNode clientApiNode = catalogConfig.get(NESSIE_CLIENT_API_VERSION_KEY);
    JsonNode refNode = catalogConfig.get(NESSIE_CATALOG_REFERENCE_KEY);
    JsonNode authTypeNode = catalogConfig.get(NESSIE_CATALOG_AUTHENTICATION_TYPE_KEY);
    this.token = null != tokenNode ? tokenNode.asText() : null;
    this.nessieApiVersion = null != clientApiNode ? clientApiNode.asText() : null;
    this.uri = catalogConfig.get(NESSIE_CATALOG_URI_CONFIG_KEY).asText();
    this.ref = null != refNode ? refNode.asText() : null;
    this.authenticationType = null != authTypeNode ? authTypeNode.asText() : "NONE";
    Preconditions.checkArgument(Arrays.asList(SUPPORTED_AUTH_TYPES).contains(this.authenticationType),
        "[NONE, BEARER] are the only supported authentication types");
  }

  @Override
  public Map<String, String> sparkConfigMap() {
    Map<String, String> configMap = new HashMap<>();
    configMap.put("spark.network.timeout", "300000");
    configMap.put("spark.sql.defaultCatalog", CATALOG_NAME);
    configMap.put("spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions");
    configMap.put("spark.sql.catalog." + CATALOG_NAME, "org.apache.iceberg.spark.SparkCatalog");
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".catalog-impl", "org.apache.iceberg.nessie.NessieCatalog");
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".uri", this.uri);
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".ref", this.ref);
    configMap.put("spark.sql.catalog." + CATALOG_NAME + ".authentication.type", this.authenticationType);
    configMap.put("spark.driver.extraJavaOptions", "-Dpackaging.type=jar -Djava.io.tmpdir=/tmp");

    configMap.putAll(this.storageConfig.sparkConfigMap(CATALOG_NAME));
    return configMap;
  }

  @Override
  public Catalog genCatalog() {
    NessieCatalog catalog = new NessieCatalog();
    Map<String, String> properties = new HashMap<>(this.storageConfig.catalogInitializeProperties());
    properties.put(CatalogProperties.URI, this.uri);
    properties.put(CatalogProperties.WAREHOUSE_LOCATION, this.storageConfig.getWarehouseUri());
    if (isNotBlank(this.ref)) {
      properties.put("ref", this.ref);
    }
    if (isNotBlank(this.authenticationType)) {
      properties.put(NESSIE_CATALOG_AUTHENTICATION_TYPE_KEY, this.authenticationType);
    }
    if (isNotBlank(this.nessieApiVersion)) {
      properties.put("client-api-version", this.nessieApiVersion);
    }
    if (isNotBlank(this.token)) {
      properties.put(CONF_NESSIE_AUTH_TOKEN, this.token);
    }
    catalog.initialize(CATALOG_NAME, properties);
    return catalog;
  }

}
