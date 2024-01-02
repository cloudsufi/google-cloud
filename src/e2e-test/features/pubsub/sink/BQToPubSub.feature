@PubSub_Sink @PUBSUB_SINK_TEST
Feature: PubSub-Sink - Verification of BigQuery to PubSub successful data transfer

  @CMEK @BQ_SOURCE_TEST
  Scenario: To verify data is getting transferred from BigQuery to PubSub successfully
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is PubSub
    Then Connect source as "BigQuery" and sink as "GooglePublisher" to establish connection
    Then Open BigQuery source properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery source mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open the PubSub sink properties
    Then Enter PubSub property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter PubSub property reference name
    Then Enter PubSub sink property topic name
    Then Select PubSub property format "csv"
    Then Enter PubSub sink property encryption key name "cmekPubSub" if cmek is enabled
    Then Enter PubSub sink property MaximumBatchCount "pubSubMaximumBatchCount"
    Then Enter PubSub sink property MaximumBatchSize "pubSubMaximumBatchSize"
    Then Enter PubSub sink property PublishDelayThreshold "pubSubPublishDelayThreshold"
    Then Enter PubSub sink property RetryTimeOut "pubSubRetryTimeOut"
    Then Enter PubSub sink property ErrorThreshold "pubSubErrorThreshold"
    Then Validate "PubSub" plugin properties
    Then Close the PubSub properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate OUT record count is equal to IN record count
    Then Open and capture logs
    Then Validate the cmek key "cmekPubSub" of target PubSub topic if cmek is enabled

  @PUBSUB_SOURCE_TEST @PUBSUB_SINK_TEST @PUBSUB_SUBSCRIPTION_TEST
  Scenario: pubsub flow
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
    Then Enter PubSub source property topic name
    Then Validate "Pub/Sub" plugin properties
    And Close the Plugin Properties page

    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
   # Then Wait till pipeline is in running state
    #Then Open and capture logs
    #Then Verify the pipeline status is "Succeeded"
    Then Publish the message
    Then Subscribe to the messages
