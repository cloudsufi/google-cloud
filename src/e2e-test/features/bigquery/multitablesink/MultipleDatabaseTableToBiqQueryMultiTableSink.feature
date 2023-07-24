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
Feature: BigQueryMultiTable sink -Verification of MultipleDatabaseTable to BigQueryMultiTable successful data transfer

  @MULTIPLEDATABASETABLE_SOURCE_DATATYPES_TEST
  Scenario:Verify data is getting transferred from MultipleDatabaseTable to BQMT sink with all datatypes
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
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Verify toggle plugin property: "truncateTable" is toggled to: "true"
    Then Verify toggle plugin property: " allow flexible schema" is toggled to: "true"
    Then Verify toggle plugin property: "update table schema" is toggled to: "true"
    Then Validate "BigQueryMultiTable" plugin properties
    And Close the Plugin Properties page
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

  @MULTIPLEDATABASETABLE_SOURCE_DATATYPES_TEST
  Scenario:Verify data is getting transferred from BigQuery to BQMT sink with split field
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
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Enter input plugin property: "dataset" with value: "dataset"
    Then Override Service account details if set in environment variables
    Then Verify toggle plugin property: "truncateTable" is toggled to: "true"
    Then Enter input plugin property: "splitField" with value: "bqmtSplitField"
    Then Verify toggle plugin property: " allow flexible schema" is toggled to: "true"
    Then Verify toggle plugin property: "update table schema" is toggled to: "true"
    Then Validate "BigQueryMultiTable" plugin properties
    And Close the Plugin Properties page
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