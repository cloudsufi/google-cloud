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

import io.cdap.e2e.utils.CdfHelper;
import io.cdap.e2e.utils.ConstantsUtil;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cdap.plugin.common.stepsdesign.TestSetupHooks;
import io.cdap.plugin.pubsub.actions.PubSubActions;
import io.cdap.plugin.utils.PubSubClient;
import io.cucumber.java.en.Then;
import io.cucumber.java.en.When;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * PubSub Source Plugin related step design.
 */

public class PubSubSource implements CdfHelper {

  @When("Source is PubSub")
  public void sourceIsPubSub() {
    selectSourcePlugin("GooglePublisher");
  }

  @Then("Open the PubSub source properties")
  public void openThePubSubSourceProperties() {
    openSourcePluginProperties("pubsub");
  }

  @Then("Enter PubSub source property topic name")
  public void enterPubSubSourcePropertyTopicName() {
    PubSubActions.enterPubSubTopic(TestSetupHooks.pubSubSourceTopic); }
  @Then("Enter PubSub source property subscription name")
  public void enterPubSubSourcePropertySubscriptionName() {
    PubSubActions.enterSubscription(TestSetupHooks.pubSubSourceSubscription);
  }

  @Then("Publish the message")
  public void publishTheMessage() throws IOException, InterruptedException {
    TimeUnit time = TimeUnit.SECONDS;
    time.sleep(120);
    PubSubClient.publishWithErrorHandlerExample(PluginPropertyUtils.pluginProp(ConstantsUtil.PROJECT_ID),
                                                TestSetupHooks.pubSubSourceTopic);
  }
}
