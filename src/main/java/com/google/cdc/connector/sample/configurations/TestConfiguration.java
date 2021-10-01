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

public class TestConfiguration {

  private final String projectId;
  private final String instanceId;
  private final String databaseId;
  private final String metadataInstanceId;
  private final String metadataDatabaseId;
  private final String changeStreamName;

  public TestConfiguration(
      String projectId,
      String instanceId,
      String databaseId,
      String metadataInstanceId,
      String metadataDatabaseId,
      String changeStreamName) {
    this.projectId = projectId;
    this.instanceId = instanceId;
    this.databaseId = databaseId;
    this.metadataInstanceId = metadataInstanceId;
    this.metadataDatabaseId = metadataDatabaseId;
    this.changeStreamName = changeStreamName;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getInstanceId() {
    return instanceId;
  }

  public String getDatabaseId() {
    return databaseId;
  }

  public String getMetadataInstanceId() {
    return metadataInstanceId;
  }

  public String getMetadataDatabaseId() {
    return metadataDatabaseId;
  }

  public String getChangeStreamName() {
    return changeStreamName;
  }
}
