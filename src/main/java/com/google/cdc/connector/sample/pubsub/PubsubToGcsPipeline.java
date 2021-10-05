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

import static com.google.cdc.connector.sample.pubsub.PubsubPipeline.EXPERIMENTS;
import static com.google.cdc.connector.sample.pubsub.PubsubPipeline.REGION;
import static com.google.cdc.connector.sample.pubsub.PubsubPipeline.TEST_CONFIGURATION;
import static com.google.cdc.connector.sample.pubsub.PubsubPipeline.deduplicateFilesToStage;
import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;

public class PubsubToGcsPipeline {

  public static final String SUBSCRIPTION = "projects/cloud-spanner-backups-loadtest/subscriptions/thiagotnunes-subscription";

  public static void main(String[] args) {
    final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(TEST_CONFIGURATION.getProjectId());
    options.setRegion(REGION);
    options.setAutoscalingAlgorithm(NONE);
    options.setRunner(DataflowRunner.class);
    options.setNumWorkers(1);
    options.setExperiments(new ArrayList<>(EXPERIMENTS));
    final List<String> filesToStage = deduplicateFilesToStage(options);
    options.setFilesToStage(filesToStage);

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(PubsubIO.readStrings().fromSubscription(SUBSCRIPTION))
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext context, OutputReceiver<String> outputReceiver) {
            final String[] fields = context.element().split(",");
            final com.google.cloud.Timestamp commitTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[1]);
            final com.google.cloud.Timestamp publishTime = com.google.cloud.Timestamp.parseTimestamp(context.timestamp().toString());

            final long committedToPublished = TimestampConverter.millisBetween(commitTimestamp, publishTime);

            outputReceiver.output(committedToPublished + "");
          }
        }))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(10))))
        .apply(TextIO
            .write()
            .to("gs://thiagotnunes-cdc-loadtest/2021-10-04/committed-to-published-")
            .withSuffix(".txt")
            .withWindowedWrites()
            .withNumShards(1)
        );

    pipeline.run().waitUntilFinish();
  }

}
