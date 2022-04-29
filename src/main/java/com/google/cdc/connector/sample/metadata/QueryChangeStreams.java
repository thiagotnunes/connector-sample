/*
 * Copyright 2022 Google LLC
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

import com.google.cdc.connector.sample.configurations.TestConfiguration;
import com.google.cdc.connector.sample.configurations.TestConfigurations;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Struct;
import java.util.HashSet;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.PartitionMetadata.State;

public class QueryChangeStreams {
  private static final String HOST = "https://staging-wrenchworks.sandbox.googleapis.com";
  private static final TestConfiguration TEST_CONFIGURATION = TestConfigurations.LOAD_TEST_3;

  public static void main(String[] args) {
    final SpannerOptions options = SpannerOptions
        .newBuilder()
        .setProjectId(TEST_CONFIGURATION.getProjectId())
        .setHost(HOST)
        .build();
    final Spanner spanner = options.getService();
    final DatabaseClient databaseClient = spanner.getDatabaseClient(DatabaseId.of(
        TEST_CONFIGURATION.getProjectId(),
        TEST_CONFIGURATION.getInstanceId(),
        TEST_CONFIGURATION.getDatabaseId()
    ));
    final PartitionMetadata partitionMetadata = PartitionMetadata
        .newBuilder()
        .setPartitionToken("AV1oK4WLahp8nUQZge47ok6RTUlEn4ffGjvzXNzhMgXyR5dLT_RQAF935ZR2ZV2hhZnjM--jyQlvx7rKv__eYpHoVNVvMo-vGEwaY2xX0877B4JIMkt06UcAeZk3Llo78RcxNabJoHIZkPmq5pDa1vpeSK93qPyBxvbU")
        .setParentTokens(new HashSet<>())
        .setStartTimestamp(Timestamp.parseTimestamp("2022-02-26T05:50:00.425000000Z"))
        .setEndTimestamp(Timestamp.parseTimestamp("9999-12-31T23:59:59.999999998Z"))
        .setHeartbeatMillis(2_000L)
        .setState(State.RUNNING)
        .setWatermark(Timestamp.MIN_VALUE)
        .build();
    final String query =
        "SELECT * FROM READ_changeStreamAll"
            + "("
            + "   start_timestamp => @startTimestamp,"
            + "   end_timestamp => @endTimestamp,"
            + "   partition_token => @partitionToken,"
            + "   read_options => null,"
            + "   heartbeat_milliseconds => @heartbeatMillis"
            + ")";
    try (ResultSet resultSet = databaseClient.singleUse().executeQuery(Statement
        .newBuilder(query)
        .bind("startTimestamp")
        .to(partitionMetadata.getStartTimestamp())
        .bind("endTimestamp")
        .to(partitionMetadata.getEndTimestamp())
        .bind("partitionToken")
        .to(partitionMetadata.getPartitionToken())
        .bind("heartbeatMillis")
        .to(partitionMetadata.getHeartbeatMillis())
        .build()
    )) {
      while (resultSet.next()) {
        final Struct row = resultSet.getCurrentRowAsStruct();
        System.out.println(row);
      }
    }
  }

}
