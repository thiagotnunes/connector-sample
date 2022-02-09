/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cdc.connector.sample.metadata;

import static org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.PartitionMetadataAdminDao.COLUMN_START_TIMESTAMP;

import com.google.cdc.connector.sample.configurations.TestConfiguration;
import com.google.cdc.connector.sample.configurations.TestConfigurations;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseAdminClient;
import com.google.cloud.spanner.InstanceAdminClient;
import com.google.cloud.spanner.BatchClient;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import java.util.Arrays;

public class QueryMetadata {

  private static final String STAGING_HOST = "https://staging-wrenchworks.sandbox.googleapis.com";
  private static final TestConfiguration TEST_CONFIGURATION = TestConfigurations.LOAD_TEST_3;
  private static final String METADATA_TABLE = "CDC_Partitions_Metadata_change_stream_metadata_36e5b3ff_44b4_4a25_bbde_e7415bc83687";

  public static void main(String[] args) {
    final String project = TEST_CONFIGURATION.getProjectId();
    final String instance = TEST_CONFIGURATION.getMetadataInstanceId();
    final String database = TEST_CONFIGURATION.getMetadataDatabaseId();
    final SpannerOptions options = SpannerOptions
        .newBuilder()
        .setProjectId(project)
        .setHost(STAGING_HOST)
        .build();
    final Spanner spanner = options.getService();
    final DatabaseId id = DatabaseId.of(project, instance, database);
    final DatabaseClient databaseClient = spanner.getDatabaseClient(id);

    try (ResultSet resultSet = databaseClient.singleUseReadOnlyTransaction(TimestampBound.ofReadTimestamp(Timestamp.parseTimestamp("2022-01-12T04:58:14.487389000Z"))).executeQuery(Statement.of(
        // "SELECT * FROM " + METADATA_TABLE + " WHERE State != 'FINISHED' ORDER BY Watermark ASC LIMIT 10"
        // "SELECT * FROM " + METADATA_TABLE + " WHERE PartitionToken = 'AV1oK4XbMkfby9qVG5o5tXts_9GVwZBo4Q9fxCKJpR-2kSMT12-X5MrLITgnCInXbnj5uw9FGeibVESZ8Fslcujfbgvpf8-iDSio6CR30UMcLLgivfm4YjW7ky4GMjHRAJaWo-imE-WLUPfrkzgblTAT11w4gQJLPb55'"
        "SELECT * FROM "
            + METADATA_TABLE
            + " WHERE State = 'CREATED'"
            + " ORDER BY "
            + COLUMN_START_TIMESTAMP
            + " ASC"
    ))) {
      while (resultSet.next()) {
        final String partitionToken = resultSet.getString("PartitionToken");
        final Timestamp watermark = resultSet.getTimestamp("Watermark");
        final String state = resultSet.getString("State");
        System.out.println(String.join(",", Arrays.asList(partitionToken, watermark.toString(), state)));
      }
    }

    spanner.close();
  }
}

