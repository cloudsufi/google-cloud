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

@BigQuery_Sink
Feature: BigQuery sink - Verification of BigQuery to BigQuery successful data transfer

@BQ_SOURCE_TEST @BQ_SOURCE_VIEW_TEST @BQ_SINK_TEST
Scenario:Validate successful records transfer from BigQuery to BigQuery with partition type TIME  with Partition field and require partitioned filter true
Given Open Datafusion Project to configure pipeline
  When Expand Plugin group in the LHS plugins list: "Source"
  When Select plugin: "BigQuery" from the plugins list as: "Source"
  When Expand Plugin group in the LHS plugins list: "Sink"
  When Select plugin: "BigQuery" from the plugins list as: "Sink"
  Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
  Then Navigate to the properties page of plugin: "BigQuery"
  And Enter input plugin property: "referenceName" with value: "Reference"
  And Replace input plugin property: "project" with value: "projectId"
  And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
  And Replace input plugin property: "dataset" with value: "dataset"
  Then Override Service account details if set in environment variables
  And Enter input plugin property: "table" with value: "bqSourceTable"
  Then Click on the Get Schema button
  Then Verify the Output Schema matches the Expected Schema: "bqSourceSchema"
  Then Validate "BigQuery" plugin properties
  And Close the Plugin Properties page
  Then Navigate to the properties page of plugin: "BigQuery2"
  Then Replace input plugin property: "project" with value: "projectId"
  Then Override Service account details if set in environment variables
  Then Enter input plugin property: "datasetProject" with value: "projectId"
  Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
  Then Enter input plugin property: "dataset" with value: "dataset"
  Then Enter input plugin property: "table" with value: "bqTargetTable"
  Then Verify toggle plugin property: "truncateTable" is toggled to: "true"
  Then Verify toggle plugin property: "truncateTable" is toggled to: "true"
Then Enter BigQuery sink property partition field "bqPartitionFieldTime"
Then Validate "BigQuery" plugin properties
Then Close the BigQuery properties
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
Then Open and capture logs
Then Verify the pipeline status is "Succeeded"
Then Verify the partition table is created with partitioned on field "bqPartitionFieldTime"

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Update for table key and dedupe key
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "update"
    And Enter textarea plugin property: "relationTableKey" with value: "tableKey"
    And Enter textarea plugin property: "dedupeBy" with value: "dedupeKey"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Upsert for table key and dedupe key
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqSourceSchemaDatatype"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "upsert"
    And Enter textarea plugin property: "relationTableKey" with value: "tableKey"
    And Enter textarea plugin property: "dedupeBy" with value: "dedupeKey"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with clustering order functionality
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "BigQuery" and "BigQuery2" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Enter textarea plugin property: "clusteringOrder" with value: "clusterValue"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for BigQuery sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Get count of no of records transferred to target BigQuery Table
