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

package com.google.cdc.connector.sample.pubsub;

import static com.google.cdc.connector.sample.configurations.TestConfigurations.PUBSUB_TOPIC;
import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

import com.google.cdc.connector.sample.configurations.TestConfiguration;
import com.google.cdc.connector.sample.configurations.TestConfigurations;
import com.google.cloud.Timestamp;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider.StaticValueProvider;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;

public class PubsubPipeline {

  public static final String SPANNER_HOST = "https://staging-wrenchworks.sandbox.googleapis.com";
  public static final String REGION = "us-central1";
  public static final int NUM_WORKERS = 100;
  public static final List<String> EXPERIMENTS = Arrays.asList(
      "use_unified_worker", "use_runner_v2"
  );
  public static final TestConfiguration TEST_CONFIGURATION = TestConfigurations.LOAD_TEST_3;

  public static void main(String[] args) {
    final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(TEST_CONFIGURATION.getProjectId());
    options.setRegion(REGION);
    options.setAutoscalingAlgorithm(NONE);
    options.setRunner(DataflowRunner.class);
    options.setNumWorkers(NUM_WORKERS);
    options.setExperiments(new ArrayList<>(EXPERIMENTS));
    final List<String> filesToStage = filesToStage(options);
    options.setFilesToStage(filesToStage);

    final Pipeline pipeline = Pipeline.create(options);

    final SpannerConfig spannerConfig = SpannerConfig
        .create()
        .withHost(StaticValueProvider.of(SPANNER_HOST))
        .withProjectId(TEST_CONFIGURATION.getProjectId())
        .withInstanceId(TEST_CONFIGURATION.getInstanceId())
        .withDatabaseId(TEST_CONFIGURATION.getDatabaseId());
    final Timestamp now = Timestamp.now();
    final Timestamp startTime = Timestamp.ofTimeSecondsAndNanos(now.getSeconds() + 300, now.getNanos());
    final Timestamp endTime = Timestamp.ofTimeSecondsAndNanos(startTime.getSeconds() + 60, startTime.getNanos());

    pipeline
        .apply(SpannerIO
            .readChangeStream()
            .withSpannerConfig(spannerConfig)
            .withChangeStreamName(TEST_CONFIGURATION.getChangeStreamName())
            .withMetadataInstance(TEST_CONFIGURATION.getMetadataInstanceId())
            .withMetadataDatabase(TEST_CONFIGURATION.getMetadataDatabaseId())
            .withInclusiveStartAt(startTime)
            .withInclusiveEndAt(endTime)
            .withQueryInterval(Duration.standardSeconds(30))
        )
        .apply(Reshuffle.viaRandomKey())
        .apply(MapElements.into(TypeDescriptors.strings()).via(record -> {
          Timestamp currentTime = Timestamp.now();
          return String.join(",", Arrays.asList(
              record.getPartitionToken(),
              record.getCommitTimestamp().toString(),
              currentTime.toString()
          ));
        }))
        .apply(Reshuffle.viaRandomKey())
        .apply(PubsubIO.writeStrings().to(PUBSUB_TOPIC));

    pipeline.run().waitUntilFinish();
  }


  private static List<String> filesToStage(DataflowPipelineOptions options) {
    final Map<String, String> fileNameToPath = new HashMap<>();
    final List<String> filePaths =
        detectClassPathResourcesToStage(DataflowRunner.class.getClassLoader(), options);

    for (String filePath : filePaths) {
      final File file = new File(filePath);
      final String fileName = file.getName();
      if (!fileNameToPath.containsKey(fileName)) {
        fileNameToPath.put(fileName, filePath);
      }
    }

    return new ArrayList<>(fileNameToPath.values());
  }
}
