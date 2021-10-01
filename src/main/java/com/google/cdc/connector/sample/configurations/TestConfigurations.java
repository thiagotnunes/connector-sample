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

package com.google.cdc.connector.sample.configurations;

public class TestConfigurations {

  public static TestConfiguration LOAD_TEST_1 = new TestConfiguration(
      "cloud-spanner-backups-loadtest",
      "change-stream-load-test-1",
      "synthetic-load-test-db",
      "change-stream-connector-md",
      "change-stream-metadata",
      "changeStreamAll"
  );

  public static TestConfiguration LOAD_TEST_2 = new TestConfiguration(
      "cloud-spanner-backups-loadtest",
      "change-stream-load-test-2",
      "testdbload-test-change-stream",
      "change-stream-connector-md",
      "change-stream-metadata",
      "changeStreamAll"
  );

  public static TestConfiguration LOAD_TEST_3 = new TestConfiguration(
      "cloud-spanner-backups-loadtest",
      "change-stream-load-test-3",
      "load-test-change-stream-enable",
      "change-stream-connector-md",
      "change-stream-metadata",
      "changeStreamAll"
  );

  public static TestConfiguration VERIFICATION = new TestConfiguration(
      "cloud-spanner-backups-loadtest",
      "cdc-hengfeng-test",
      "testdbverification-test-db",
      "change-stream-load-test-3",
      "change-stream-meta-hengfeng",
      "changeStreamAll"
  );

  public static final String PUBSUB_TOPIC = "projects/cloud-spanner-backups-loadtest/topics/thiagotnunes-cdc-test";
  public static final String PUBSUB_SUBSCRIPTION = "thiagotnunes-subscription";
}
