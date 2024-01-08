# Copyright © 2023 Cask Data, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

@PubSub_Source
Feature: PubSub Source - Verification of PubSub to PubSub successful data transfer in different formats.

  @PUBSUB_SOURCE_TEST @PUBSUB_SINK_TEST @PUBSUB_SUBSCRIPTION_TEST
  Scenario: Validate successful transfer of records from PubSub(source) to PubSub(sink).
    Given Open Datafusion Project to configure pipeline
    When Select data pipeline type as: "Realtime"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Pub/Sub" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Pub/Sub" from the plugins list as: "Sink"
    Then Connect plugins: "Pub/Sub" and "Pub/Sub2" to establish connection
    Then Navigate to the properties page of plugin: "Pub/Sub"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter PubSub source property subscription name
    Then Enter PubSub source property topic name
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    And Click on configure button
    And Click on pipeline config
    And Click on batch time and select format
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    Then Publish the messages
    Then Subscribe to the messages
    Then Validate OUT record count is equal to IN record count
    And Stop the pipeline
    Then Verify the pipeline status is "Stopped"

  @PUBSUB_SOURCE_TEST @PUBSUB_SINK_TEST @PUBSUB_SUBSCRIPTION_TEST
    Scenario: Validate successful transfer of records from PubSub(source) to PubSub(sink) using macros.
      Given Open Datafusion Project to configure pipeline
      When Select data pipeline type as: "Realtime"
      When Expand Plugin group in the LHS plugins list: "Source"
      When Select plugin: "Pub/Sub" from the plugins list as: "Source"
      When Expand Plugin group in the LHS plugins list: "Sink"
      When Select plugin: "Pub/Sub" from the plugins list as: "Sink"
      Then Connect plugins: "Pub/Sub" and "Pub/Sub2" to establish connection
      Then Navigate to the properties page of plugin: "Pub/Sub"
      Then Override Service account details if set in environment variables
      Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
      Then Click on the Macro button of Property: "topic" and set the value in textarea: "PubSubSourceTopic"
      Then Click on the Macro button of Property: "subscription" and set the value in textarea: "PubSubSourceSubscription"
      Then Validate "Pub/Sub" plugin properties
      And Close the Plugin Properties page
      Then Open the PubSub sink properties
      Then Enter PubSub property projectId "projectId"
      Then Override Service account details if set in environment variables
      Then Enter PubSub property reference name
      Then Enter PubSub sink property topic name
      Then Validate "Pub/Sub" plugin properties
      And Close the Plugin Properties page
      And Click on configure button
      And Click on pipeline config
      And Click on batch time and select format
      Then Save the pipeline
      Then Enter runtime argument value for PubSub source property topic key "PubSubSourceTopic"
      Then Enter runtime argument value for PubSub source property subscription key "PubSubSourceSubscription"
      Then Deploy the pipeline
      Then Run the Pipeline in Runtime with runtime arguments
      Then Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
      Then Publish the messages
      Then Subscribe to the messages
      Then Validate OUT record count is equal to IN record count
      And Stop the pipeline
      Then Verify the pipeline status is "Stopped"

  @PUBSUB_SOURCE_TEST @PUBSUB_SINK_TEST @PUBSUB_SUBSCRIPTION_TEST
  Scenario: Validate successful transfer of records from PubSub(source) to PubSub(sink)  having formats TEXT at both source and sink.
    Given Open Datafusion Project to configure pipeline
    When Select data pipeline type as: "Realtime"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Pub/Sub" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Pub/Sub" from the plugins list as: "Sink"
    Then Connect plugins: "Pub/Sub" and "Pub/Sub2" to establish connection
    Then Navigate to the properties page of plugin: "Pub/Sub"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter PubSub source property subscription name
    Then Enter PubSub source property topic name
    Then Select dropdown plugin property: "select-format" with option value: "text"
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select dropdown plugin property: "select-format" with option value: "text"
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    And Click on configure button
    And Click on pipeline config
    And Click on batch time and select format
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    Then Publish the messages
    Then Subscribe to the messages
    Then Validate OUT record count is equal to IN record count
    And Stop the pipeline
    Then Verify the pipeline status is "Stopped"

  @PUBSUB_SOURCE_TEST @PUBSUB_SINK_TEST @PUBSUB_SUBSCRIPTION_TEST
  Scenario: Validate successful transfer of records from PubSub(source) to PubSub(sink)  having formats TEXT at both source and sink.
    Given Open Datafusion Project to configure pipeline
    When Select data pipeline type as: "Realtime"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Pub/Sub" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Pub/Sub" from the plugins list as: "Sink"
    Then Connect plugins: "Pub/Sub" and "Pub/Sub2" to establish connection
    Then Navigate to the properties page of plugin: "Pub/Sub"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter PubSub source property subscription name
    Then Enter PubSub source property topic name
    Then Select dropdown plugin property: "select-format" with option value: "text"
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select dropdown plugin property: "select-format" with option value: "text"
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page
    And Click on configure button
    And Click on pipeline config
    And Click on batch time and select format
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    Then Publish the messages
    Then Subscribe to the messages
    Then Validate OUT record count is equal to IN record count
    And Stop the pipeline
    Then Verify the pipeline status is "Stopped"