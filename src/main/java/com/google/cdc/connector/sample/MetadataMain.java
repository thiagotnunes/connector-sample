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

package com.google.cdc.connector.sample;

import static com.google.cdc.connector.sample.PipelineMain.METADATA_DATABASE;
import static com.google.cdc.connector.sample.PipelineMain.METADATA_INSTANCE;
import static com.google.cdc.connector.sample.PipelineMain.PROJECT_ID;
import static com.google.cdc.connector.sample.PipelineMain.SPANNER_HOST;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.util.AbstractMap.SimpleEntry;
import java.util.Map;
import java.util.Map.Entry;

public class MetadataMain {

  private static final String TABLE = "CDC_Partitions_change_stream_metadata_8ccacdb2_c4c9_430d_b752_6a49338f5d82";

  public static void main(String[] args) throws InterruptedException {
    final SpannerOptions options = SpannerOptions
        .newBuilder()
        .setHost(SPANNER_HOST)
        .setProjectId(PROJECT_ID)
        .build();
    final Spanner spanner = options.getService();
    final DatabaseId id = DatabaseId.of(PROJECT_ID, METADATA_INSTANCE, METADATA_DATABASE);
    final DatabaseClient databaseClient = spanner.getDatabaseClient(id);

    while (true) {
      final Entry<String, Timestamp> min = queryCurrentWatermark(databaseClient, "ASC");
      final Entry<String, Timestamp> max = queryCurrentWatermark(databaseClient, "DESC");
      if (min == null || max == null) continue;
      System.out.println(min.getValue() + ", " + max.getValue());
      Thread.sleep(5_000L);
    }
  }

  private static Map.Entry<String, Timestamp> queryCurrentWatermark(DatabaseClient databaseClient, String order) {
    try (ResultSet resultSet = databaseClient
        .singleUse()
        .executeQuery(Statement.of("SELECT * FROM " + TABLE + " ORDER BY CurrentWatermark " + order + " LIMIT 1"))) {
      if (resultSet.next()) {
        return new SimpleEntry<>(resultSet.getString("PartitionToken"), resultSet.getTimestamp("CurrentWatermark"));
      }
    }
    return null;
  }
}
