/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.utils;

import com.google.api.gax.rpc.AlreadyExistsException;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import com.google.pubsub.v1.ProjectSubscriptionName;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Represents PubSub client.
 */
public class PubSubClient {

  public static Topic createTopic(String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      return topicAdminClient.createTopic(topicName);
    }
  }

  // Create the subscription
  public static ProjectSubscriptionName createSubscription(String subscriptionId, String topicId) throws IOException {
    ProjectSubscriptionName subscriptionName = null;
    try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(
      SubscriptionAdminSettings.newBuilder().build())) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      subscriptionName = ProjectSubscriptionName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), subscriptionId);
      subscriptionAdminClient.createSubscription(subscriptionName, topicName, PushConfig.getDefaultInstance(), 60);
      System.out.println("Subscription created: " + subscriptionName.toString());
    } catch (AlreadyExistsException e) {
      System.out.println("Subscription already exists: " + subscriptionName.toString());
    }
    return subscriptionName;
  }

  public static void deleteTopic(String topicId) throws IOException {
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      topicAdminClient.deleteTopic(topicName);
    }
  }

  public static Topic getTopic(String topicId) throws IOException {
    Topic topic;
    try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
      TopicName topicName = TopicName.of(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID), topicId);
      topic = topicAdminClient.getTopic(topicName);
      return topic;
    }
  }

  public static String getTopicCmekKey(String topicId) throws IOException {
    return getTopic(topicId).getKmsKeyName();
  }

  public static void subscribeAsyncExample(String projectId, String subscriptionId) {
    ProjectSubscriptionName subscriptionName =
      ProjectSubscriptionName.of(projectId, subscriptionId);

    // Instantiate an asynchronous message receiver.
    MessageReceiver receiver =
      (PubsubMessage message, AckReplyConsumer consumer) -> {
        // Handle incoming message, then ack the received message.
        System.out.println("Id: " + message.getMessageId());
        System.out.println("Data: " + message.getData().toStringUtf8());
        consumer.ack();
      };

    Subscriber subscriber = null;
    try {
      subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
      // Start the subscriber.
      subscriber.startAsync().awaitRunning();
      System.out.printf("Listening for messages on %s:\n", subscriptionName.toString());
      // Allow the subscriber to run for 30s unless an unrecoverable error occurs.
      subscriber.awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      // Shut down the subscriber after 30s. Stop receiving messages.
      subscriber.stopAsync();
    }
  }
}
