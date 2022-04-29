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

package com.google.cdc.connector.sample.fullrow;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.SessionPoolOptions;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TimestampBound;
import java.util.Arrays;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.model.DataChangeRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.json.JSONObject;

public class ChangeRecordToFullRowJson extends DoFn<DataChangeRecord, String> {

  private static final int MIN_SESSIONS = 1;
  private static final int MAX_SESSIONS = 5;
  private final String projectId;
  private final String instanceId;
  private final String databaseId;

  private transient DatabaseClient client;
  private transient Spanner spanner;

  public ChangeRecordToFullRowJson(SpannerConfig spannerConfig) {
    this.projectId = spannerConfig.getProjectId().get();
    this.instanceId = spannerConfig.getInstanceId().get();
    this.databaseId = spannerConfig.getDatabaseId().get();
  }

  @Setup
  public void setup() {
    SessionPoolOptions sessionPoolOptions = SessionPoolOptions
        .newBuilder().setMinSessions(MIN_SESSIONS).setMaxSessions(MAX_SESSIONS).build();
    SpannerOptions options = SpannerOptions
        .newBuilder().setProjectId(projectId).setSessionPoolOption(sessionPoolOptions).build();
    DatabaseId id = DatabaseId.of(projectId, instanceId, databaseId);
    spanner = options.getService();
    client = spanner.getDatabaseClient(id);
  }

  @Teardown
  public void teardown() {
    spanner.close();
  }

  @ProcessElement
  public void process(@Element DataChangeRecord element, OutputReceiver<String> output) {
    com.google.cloud.Timestamp commitTimestamp = element.getCommitTimestamp();
    element.getMods().forEach(mod -> {
      JSONObject keysJson = new JSONObject(mod.getKeysJson());
      long singerId = keysJson.getLong("SingerId");
      try (ResultSet resultSet = client
          .singleUse(TimestampBound.ofReadTimestamp(commitTimestamp))
          .read(
              "Singers",
              KeySet.singleKey(com.google.cloud.spanner.Key.of(singerId)),
              Arrays.asList("FirstName", "LastName"))) {
        if (resultSet.next()) {
          JSONObject jsonRow = new JSONObject();
          jsonRow.put("SingerId", singerId);
          jsonRow.put("FirstName", resultSet.isNull("FirstName") ?
              JSONObject.NULL : resultSet.getString("FirstName"));
          jsonRow.put("LastName", resultSet.isNull("LastName") ?
              JSONObject.NULL : resultSet.getString("LastName"));
          output.output(jsonRow.toString());
        }
      }
    });
  }
}
