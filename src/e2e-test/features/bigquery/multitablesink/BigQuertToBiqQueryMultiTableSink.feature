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

@BigQueryMultiTable_Sink
Feature: BigQueryMultiTable sink -Verification of BigQuery to BigQueryMultiTable successful data transfer

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Verify data is getting transferred from BigQuery to BQMT sink with all datatypes
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BiqQuery Multi Table
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
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "BigQuery" and sink as "BigQueryMultiTable" to establish connection
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
    Then Validate records transferred to target table is equal to number of records from source table

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Verify data is getting transferred from BigQuery to BQMT sink with split field
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BiqQuery Multi Table
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
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Toggle BiqQueryMultiTable sink property truncateTable to "true"
    Then Enter input plugin property: "splitField" with value: "bqmtSplitField"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Select BiqQueryMultiTable sink property update table schema as "true"
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "BigQuery" and sink as "BigQueryMultiTable" to establish connection
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
    Then Validate records transferred to target table is equal to number of records from source table