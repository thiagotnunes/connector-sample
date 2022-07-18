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

package com.google.cdc.connector.sample.count;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.AfterAll;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CountPipeline {

  public static void main(String[] args) {
    final PipelineOptions options = PipelineOptionsFactory.create();
    final Pipeline p = Pipeline.create(options);

    final TestStream<String> events = TestStream.create(AvroCoder.of(String.class))
        .advanceWatermarkTo(new Instant(0L))
        .addElements("1", "2")
        .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(1)))
        .addElements("3", "4")
        .advanceWatermarkTo(new Instant(0L).plus(Duration.standardMinutes(2)))
        .addElements("5")
        .advanceWatermarkToInfinity();

    p.apply(events)
        .apply(Window
            .<String>into(new GlobalWindows())
            .triggering(AfterWatermark.pastEndOfWindow())
            .discardingFiredPanes())
        .apply(Combine.globally(Count.combineFn()))
        .apply(ParDo.of(new LogFn()));

    p.run().waitUntilFinish();
  }

  private static class LogFn extends DoFn<Long, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(LogFn.class);

    @ProcessElement
    public void processElement(@Element Long e) {
      LOGGER.info("Processed a total of " + e + " elements");
    }
  }
}
