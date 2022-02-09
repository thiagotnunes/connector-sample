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

package com.google.cdc.connector.sample.verification;

import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

import com.google.cdc.connector.sample.configurations.TestConfiguration;
import com.google.cdc.connector.sample.configurations.TestConfigurations;
import com.google.cloud.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.joda.time.Duration;

public class VerificationPipeline {

  public static final String SPANNER_HOST = "https://wrenchworks-loadtest.googleapis.com";
  public static final String REGION = "us-central1";
  public static final int NUM_WORKERS = 10;
  public static final List<String> EXPERIMENTS = Arrays.asList(
      "use_unified_worker", "use_runner_v2"
  );
  public static final TestConfiguration TEST_CONFIGURATION = TestConfigurations.LOAD_TEST_3;
  public static final String GCS_PATH_PREFIX = "gs://thiagotnunes-cdc-loadtest/change-stream-records/connector";

  public static void main(String[] args) {
    final DataflowPipelineOptions options = PipelineOptionsFactory
        .as(DataflowPipelineOptions.class);
    options.setProject(TEST_CONFIGURATION.getProjectId());
    options.setRegion(REGION);
    options.setAutoscalingAlgorithm(NONE);
    options.setRunner(DataflowRunner.class);
    options.setNumWorkers(NUM_WORKERS);
    options.setExperiments(new ArrayList<>(EXPERIMENTS));
    options.setStreaming(true);
    final Pipeline pipeline = Pipeline.create(options);

    final Timestamp startTime = Timestamp.now();

    pipeline
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(SpannerConfig
                .create()
                .withHost(StaticValueProvider.of(SPANNER_HOST))
                .withProjectId(TEST_CONFIGURATION.getProjectId())
                .withInstanceId(TEST_CONFIGURATION.getInstanceId())
                .withDatabaseId(TEST_CONFIGURATION.getDatabaseId())
            )
            .withMetadataDatabase(TEST_CONFIGURATION.getMetadataDatabaseId())
            .withMetadataInstance(TEST_CONFIGURATION.getMetadataInstanceId())
            .withChangeStreamName(TEST_CONFIGURATION.getChangeStreamName())
            .withInclusiveStartAt(startTime)
        )
        .apply(
            "Log messages",
            MapElements
                .into(TypeDescriptor.of(String.class))
                .via(record -> String.format(
                    "%s, %s, %s, %d",
                    record.getPartitionToken(),
                    record.getCommitTimestamp(),
                    record.getServerTransactionId(),
                    record.getNumberOfRecordsInTransaction()
                )))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply("Write records to GCS", new WriteOneFilePerWindow(GCS_PATH_PREFIX, 5));

    pipeline.run().waitUntilFinish();
  }
}
