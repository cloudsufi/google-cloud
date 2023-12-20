
Feature: PubSub-Sink - Verification of BigQuery to PubSub successful data transfer

  @PUBSUB_SOURCE_TEST @PUBSUB_SUBSCRIPTION_TEST @PUBSUB_SINK_TEST
  Scenario: To verify data is getting transferred from BigQuery to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    And Select data pipeline type as: "Realtime"
    When Source is PubSub Realtime
    When Sink is PubSub
    And Connect plugins: "Pub/Sub" and "Pub/Sub2" to establish connection
    And Navigate to the properties page of plugin: "Pub/Sub"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter input plugin property: "project" with value: "cdf-athena"
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
    Then Save the pipeline
    Then Preview and run the pipeline

