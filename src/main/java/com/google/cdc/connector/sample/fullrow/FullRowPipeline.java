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

import com.google.cloud.Timestamp;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.ParDo;

public class FullRowPipeline {

  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();

    SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withProjectId("my-project-id")
        .withInstanceId("my-instance-id")
        .withDatabaseId("my-database-id");
    pipeline
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName("my-change-stream")
            .withMetadataInstance("my-metadata-instance-id")
            .withMetadataDatabase("my-metadata-database-id")
            .withInclusiveStartAt(Timestamp.now()))
        .apply(ParDo.of(new ChangeRecordToFullRowJson(spannerConfig)));

    pipeline.run().waitUntilFinish();
  }
}
