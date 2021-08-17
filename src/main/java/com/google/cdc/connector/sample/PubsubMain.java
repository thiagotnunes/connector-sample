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


import static com.google.cdc.connector.sample.PipelineMain.PROJECT_ID;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Charsets;

public class PubsubMain {

  private static final String SUBSCRIPTION_ID = "thiagotnunes-subscription";

  public static void main(String[] args) {
    final ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(PROJECT_ID, SUBSCRIPTION_ID);

    final MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
      final String messageId = message.getMessageId();
      final String data = message.getData().toString(Charsets.UTF_8);
      System.out.println("Received message " + messageId + " with data " + data);
      consumer.ack();
    };

    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
      subscriber.startAsync().awaitRunning();
      System.out.println("Listening for messages on " + subscriptionName);
      subscriber.awaitTerminated(30, TimeUnit.MINUTES);
    } catch (TimeoutException e) {
      subscriber.stopAsync();
    }
  }
}
