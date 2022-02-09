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


import static com.google.cdc.connector.sample.configurations.TestConfigurations.PUBSUB_SUBSCRIPTION;
import static com.google.cdc.connector.sample.pubsub.PubsubPipeline.TEST_CONFIGURATION;

import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class PubsubConsumer {

  public static void main(String[] args) throws IOException {
    final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(TEST_CONFIGURATION.getProjectId(), PUBSUB_SUBSCRIPTION);
    final OutputFile outputFile = new OutputFile("one_hour_percentiles.txt", true);
    final PercentileMessageReceiver percentileReceiver = new PercentileMessageReceiver(60_000_000, outputFile);

    outputFile.open();
    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, percentileReceiver).build();
      subscriber.startAsync().awaitRunning();
      System.out.println("Listening for messages on " + subscriptionName);
      subscriber.awaitTerminated(300, TimeUnit.MINUTES);
      percentileReceiver.printPercentiles();
    } catch (TimeoutException e) {
      subscriber.stopAsync();
    } finally {
      outputFile.close();
    }
  }

}
