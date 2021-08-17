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

import static com.google.cdc.connector.sample.PipelineMain.EXPERIMENTS;
import static com.google.cdc.connector.sample.PipelineMain.PROJECT_ID;
import static com.google.cdc.connector.sample.PipelineMain.REGION;
import static com.google.cdc.connector.sample.PipelineMain.TOPIC;
import static org.apache.beam.runners.core.construction.resources.PipelineResources.detectClassPathResourcesToStage;
import static org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType.NONE;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;

public class PubsubPipelineMain {
  public static void main(String[] args) {
    final DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
    options.setProject(PROJECT_ID);
    options.setRegion(REGION);
    options.setAutoscalingAlgorithm(NONE);
    options.setRunner(DataflowRunner.class);
    options.setNumWorkers(1);
    options.setExperiments(new ArrayList<>(EXPERIMENTS));
    final List<String> filesToStage = filesToStage(options);
    options.setFilesToStage(filesToStage);

    final Pipeline pipeline = Pipeline.create(options);

    pipeline
        .apply(Create.of("1", "2", "3", "4", "5"))
        .apply(PubsubIO.writeStrings().to(TOPIC));

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
