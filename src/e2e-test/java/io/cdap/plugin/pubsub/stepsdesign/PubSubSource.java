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

package io.cdap.plugin.pubsub.stepsdesign;

import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.pubsub.actions.PubSubActions;
import io.cdap.e2e.utils.CdfHelper;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;

import java.io.IOException;

/**
 * PubSub Source Plugin related step design.
 */

public class PubSubSource implements CdfHelper {

  @When("Source is PubSub")
  public void sourceIsPubSub() {
    selectSourcePlugin("GooglePublisher");
  }

  @When("Source is PubSub Realtime")
  public void sourceIsPubSubRealtime() {
    PubSubActions.selectPubSubRealtimePlugin();
  }

  @Then("Open the PubSub source properties")
  public void openThePubSubSourceProperties() {
    openSourcePluginProperties("GooglePublisher");
  }

  @Then("Create a topic in PubSub")
  public void createATopicInPubSub() {
    PubSubActions.enterSourceTopic(TestSetupHooks.pubSubSourceTopic);
  }

  @Then("Create a Subscription in PubSub")
  public void createASubscriptionInPubSub() {
    PubSubActions.enterSourceSubscription(TestSetupHooks.pubSubSourceSubscription);
  }

  @Then("Publish a message")
  public void publishAMessage() throws IOException, InterruptedException {
    Thread.sleep(500);
    TestSetupHooks.createMessagePubSubTopic();
  }
}
