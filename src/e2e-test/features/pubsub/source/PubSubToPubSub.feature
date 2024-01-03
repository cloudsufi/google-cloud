
Feature: PubSub-Sink - Verification of BigQuery to PubSub successful data transfer

  @PUBSUB_SOURCE_TEST @PUBSUB_SUBSCRIPTION_TEST @PUBSUB_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    When Source is PubSub Realtime
    When Sink is PubSub
    And Connect plugins: "GoogleSubscriber" and "Pub/Sub" to establish connection
    And Navigate to the properties page of plugin: "GoogleSubscriber"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Create a topic in PubSub
    Then Create a Subscription in PubSub
    Then Click on the Validate button
    Then Close the Plugin Properties page
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    And Save and Deploy Pipeline
    And Run the Pipeline in Runtime
    And Wait for pipeline to be in status: "Running" with a timeout of 240 seconds
    And Publish the message
    And Subscribe the message
    And Stop the pipeline
    And Open and capture logs
    Then Verify the pipeline status is "Stopped"

