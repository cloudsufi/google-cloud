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

  @MULTIPLEDATABASETABLE_SOURCE_DATATYPES_TEST
  Scenario:Verify data is getting transferred from BigQuery to BQMT sink with all datatypes using macros
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "MultiTableDatabase" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQueryMultiTable" from the plugins list as: "Sink"
    Then Connect plugins: "MultiTableDatabase" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "MultiTableDatabase"
    And Enter input plugin property: "referenceName" with value: "Reference"
    Then Replace input plugin property: "connectionString" with value: "connectionString" for Credentials and Authorization related fields
    Then Replace input plugin property: "jdbcPluginName" with value: "jdbcPluginName" for Credentials and Authorization related fields
    Then Replace input plugin property: "user" with value: "user" for Credentials and Authorization related fields
    Then Replace input plugin property: "password" with value: "password" for Credentials and Authorization related fields
    Then Enter textarea plugin property: "sqlStatements" with value: "selectQuery"
    And Enter input plugin property: "tableNameField" with value: "tableNameField"
    Then Verify the Output Schema matches the Expected Schema: "datatypesSchema"
    Then Validate "MultiTableDatabase" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQueryMultiTable"
    And Enter input plugin property: "referenceName" with value: "Reference"
    Then Click on the Macro button of Property: "projectId" and set the value in textarea: "bqProjectId"
    Then Click on the Macro button of Property: "datasetProjectId" and set the value in textarea: "bqDatasetProjectId"
    Then Click on the Macro button of Property: "serviceAccountType" and set the value in textarea: "serviceAccountType"
    Then Click on the Macro button of Property: "serviceAccountFilePath" and set the value in textarea: "serviceAccount"
    Then Click on the Macro button of Property: "dataset" and set the value in textarea: "bqDataset"
    Then Verify toggle plugin property: "truncateTable" is toggled to: "true"
    Then Verify toggle plugin property: "allow flexible schema" is toggled to: "true"
    Then Verify toggle plugin property: "allowSchemaRelaxation" is toggled to: "true"
    Then Validate "BigQueryMultiTable" plugin properties
    And Close the Plugin Properties page
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
