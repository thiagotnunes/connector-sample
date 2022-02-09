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
import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

import java.util.ArrayList;
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
    options.setNumWorkers(30);
    options.setExperiments(new ArrayList<>(EXPERIMENTS));

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(PubsubIO.readStrings().fromSubscription(SUBSCRIPTION))
        .apply(ParDo.of(new DoFn<String, String>() {
          @ProcessElement
          public void processElement(ProcessContext context, OutputReceiver<String> outputReceiver) {
            final String[] fields = context.element().split(",");
            final com.google.cloud.Timestamp commitTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[1]);
            final com.google.cloud.Timestamp emitTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[2]);
            final com.google.cloud.Timestamp partitionCreatedTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[3]);
            final com.google.cloud.Timestamp partitionScheduledTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[4]);
            final com.google.cloud.Timestamp partitionRunningTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[5]);
            final com.google.cloud.Timestamp recordStreamStartTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[6]);
            final com.google.cloud.Timestamp recordStreamEndTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[7]);
            final com.google.cloud.Timestamp readTimestamp = com.google.cloud.Timestamp.parseTimestamp(fields[8]);
            final com.google.cloud.Timestamp publishTimestamp = com.google.cloud.Timestamp.parseTimestamp(context.timestamp().toString());

            final long committedToRead = TimestampConverter.millisBetween(commitTimestamp, readTimestamp);
            final long readToEmitted = TimestampConverter.millisBetween(readTimestamp, emitTimestamp);
            final long committedToEmitted = TimestampConverter.millisBetween(commitTimestamp, emitTimestamp);
            final long committedToPublished = TimestampConverter.millisBetween(commitTimestamp, publishTimestamp);
            final long streamStartToStreamEnd = TimestampConverter.millisBetween(recordStreamStartTimestamp, recordStreamEndTimestamp);

            final long partitionCreatedToScheduled = TimestampConverter.millisBetween(partitionCreatedTimestamp, partitionScheduledTimestamp);
            final long partitionScheduledToRunning = TimestampConverter.millisBetween(partitionScheduledTimestamp, partitionRunningTimestamp);

            outputReceiver.output("CTR " + committedToRead);
            outputReceiver.output("RTE " + readToEmitted);
            outputReceiver.output("CTE " + committedToEmitted);
            outputReceiver.output("CTP " + committedToPublished);
            outputReceiver.output("SSTSE " + streamStartToStreamEnd);

            outputReceiver.output("PCS " + partitionCreatedToScheduled);
            outputReceiver.output("PSR " + partitionScheduledToRunning);
          }
        }))
        .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
        .apply(TextIO
            .write()
            .to("gs://thiagotnunes-cdc-loadtest/2022-01-19-custom-pubsub-pitr-workers-10-2/trace-")
            .withSuffix(".txt")
            .withWindowedWrites()
            .withNumShards(1)
        );

    pipeline.run().waitUntilFinish();
  }

}
