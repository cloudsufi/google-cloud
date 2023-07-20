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
When Source is BigQuery
When Sink is BigQuery
Then Open BigQuery source properties
Then Enter BigQuery property reference name
Then Enter BigQuery property projectId "projectId"
Then Enter BigQuery property datasetProjectId "projectId"
Then Override Service account details if set in environment variables
Then Enter BigQuery property dataset "dataset"
Then Enter BigQuery source property table name
Then Validate output schema with expectedSchema "bqSourceSchemaDatatype"
Then Validate "BigQuery" plugin properties
Then Close the BigQuery properties
Then Open BigQuery sink properties
Then Enter BigQuery property reference name
Then Enter BigQuery property projectId "projectId"
Then Enter BigQuery property datasetProjectId "projectId"
Then Override Service account details if set in environment variables
Then Enter BigQuery property dataset "dataset"
Then Enter BigQuery sink property table name
Then Toggle BigQuery sink property truncateTable to true
Then Toggle BigQuery sink property updateTableSchema to true
Then Enter BigQuery sink property partition field "bqPartitionFieldTime"
  Then Toggle BigQuery sink property require partition filter to true
Then Validate "BigQuery" plugin properties
Then Close the BigQuery properties
Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
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

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Update for table key and dedupe key
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    And Select radio button plugin property: "operation" with value: "update"
    And Enter textarea plugin property: "relationTableKey" with value: "tableKey"
    And Enter textarea plugin property: "dedupeBy" with value: "dedupeKey"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
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
  Scenario:Validate successful records transfer from BigQuery to BigQuery with Advanced operations Upsert for table key and dedupe key
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    And Select radio button plugin property: "operation" with value: "upsert"
    And Enter textarea plugin property: "relationTableKey" with value: "tableKey"
    And Enter textarea plugin property: "dedupeBy" with value: "dedupeKey"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
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
    When Source is BigQuery
    When Sink is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Enter textarea plugin property: "clusteringOrder" with value: "clusterValue"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
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
    
