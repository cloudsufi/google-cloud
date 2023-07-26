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

@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to GCS successful data transfer

  @CMEK @BQ_SOURCE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Override Service account details if set in environment variables
    Then Enter BiqQuery property encryption key name "cmekBQ" if cmek is enabled
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket

  @BQ_SOURCE_DATATYPE_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery with different datatypes to GCS
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
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
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table

  @BQ_SOURCE_TEST @BQ_SOURCE_VIEW_TEST @GCS_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to GCS by enable querying views
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is GCS
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name as view
    Then Toggle BigQuery source property enable querying views to true
    Then Enter the BigQuery source property for view materialization project "projectId"
    Then Enter the BigQuery source property for view materialization dataset "dataset"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open GCS sink properties
    Then Override Service account details if set in environment variables
    Then Enter the GCS sink mandatory properties
    Then Validate "GCS" plugin properties
    Then Close the GCS properties
    Then Connect source as "BigQuery" and sink as "GCS" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Click on preview data for GCS sink
    Then Verify preview output schema matches the outputSchema captured in properties
    Then Close the preview data
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Verify data is transferred to target GCS bucket
