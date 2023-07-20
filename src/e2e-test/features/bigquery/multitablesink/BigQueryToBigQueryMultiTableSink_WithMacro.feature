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
Feature: BigQueryMultiTable sink -Verification of BigQuery to BigQueryMultiTable successful data transfer using macros

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Verify data is getting transferred from BigQuery to BQMT sink with all datatypes using macros
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    When Sink is BiqQuery Multi Table
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property "projectId" as macro argument "bqProjectId"
    Then Enter BigQuery property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQuery property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQuery property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQuery property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQuery property "dataset" as macro argument "bqDataset"
    Then Enter BigQuery property "table" as macro argument "bqSourceTable"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BigQueryMultiTable sink property "projectId" as macro argument "bqProjectId"
    Then Enter BigQueryMultiTable sink property "datasetProjectId" as macro argument "bqDatasetProjectId"
    Then Enter BigQueryMultiTable sink property "serviceAccountType" as macro argument "serviceAccountType"
    Then Enter BigQueryMultiTable sink property "serviceAccountFilePath" as macro argument "serviceAccount"
    Then Enter BigQueryMultiTable sink property "serviceAccountJSON" as macro argument "serviceAccount"
    Then Enter BigQueryMultiTable sink property "dataset" as macro argument "bqDataset"
    Then Enter BigQueryMultiTable sink property "truncateTable" as macro argument "bqTruncateTable"
    Then Toggle BiqQueryMultiTable sink property allow flexible schema to "true"
    Then Enter BigQueryMultiTable sink property "allowSchemaRelaxation" as macro argument "bqmtUpdateTableSchema"
    Then Validate "BigQueryMultiTable" plugin properties
    Then Close the BiqQueryMultiTable properties
    Then Connect source as "BigQuery" and sink as "BigQueryMultiTable" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "serviceAccountType" for key "serviceAccountType"
    Then Enter runtime argument value "serviceAccount" for key "serviceAccount"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value for BigQuery source table name key "bqSourceTable"
    Then Enter runtime argument value "projectId" for key "bqProjectId"
    Then Enter runtime argument value "projectId" for key "bqDatasetProjectId"
    Then Enter runtime argument value "dataset" for key "bqDataset"
    Then Enter runtime argument value "bqTruncateTableTrue" for key "bqTruncateTable"
    Then Enter runtime argument value "bqmtUpdateTableSchemaTrue" for key "bqmtUpdateTableSchema"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"



