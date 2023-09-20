@BigQuery_Source
Feature: BigQuery source - Verification of BigQuery to BigQuery successful data transfer

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with filter
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
    Then Enter BigQuery source property filter "bqFilter"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
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
    Then Validate records transferred to target table is equal to number of records from source table with filter "bqFilter"

  @BQ_SOURCE_TEST @BQ_SINK_TEST
  Scenario:Validate that pipeline run preview gets failed when incorrect filter values are provided
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
    Then Enter BigQuery source property filter "bqInvalidFilter"
    Then Validate output schema with expectedSchema "bqSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "failed"

  @BQ_PARTITIONED_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify records are getting transferred with respect to partitioned date
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
    Then Enter BigQuery source properties partitionStartDate and partitionEndDate
    Then Validate output schema with expectedSchema "bqPartitionSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
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
    Then Validate partition date in output partitioned table

  @BQ_PARTITIONED_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify records are not getting transferred for future partitioned date
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
    Then Enter BigQuery source properties partitionStartDate "bqFuturePartitionStartDate" and partitionEndDate "bqFuturePartitionEndDate"
    Then Validate output schema with expectedSchema "bqPartitionSourceSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Open BigQuery sink properties
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery sink mandatory properties
    Then Validate "BigQuery" plugin properties
    Then Close the BigQuery properties
    Then Connect source as "BigQuery" and sink as "BigQuery" to establish connection
    Then Save and Deploy Pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Validate the records are not created in output table
    Then Validate partitioning is not done on the output table

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with default time partitioning type
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
    Then Enter BigQuery sink property partition field "transaction_date"
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

  @BQ_SOURCE_DATATYPE_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery with all the datatypes
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table


  @BQ_SOURCE_TEST @BQ_SOURCE_VIEW_TEST @BQ_SINK_TEST
  Scenario:Validate successful records transfer from BigQuery to BigQuery by enable querying views
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
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table


  @BQ_UPSERT_SOURCE_TEST @BQ_UPSERT_SINK_TEST
  Scenario: Verify scenario form BigQuery To to ensure that upsert operations are performed without updating the destination table
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
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "upsert"
    Then Enter Value for plugin property table key : "relationTableKey" with values: "string_value"
    Then Select dropdown plugin property: "dedupeBy" with option value: "DESC"
    Then Enter key for plugin property: "dedupeBy" with values: "float_value"
    Then Click plugin property: "updateTableSchema"
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



  @BQ_UPDATE_SOURCE_TEST @BQ_UPDATE_SINK_TEST
  Scenario: Verify form BigQuery To to ensure that update operations are performed and the ensure that the duplicate entries has been removed to sink.
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
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    And Select radio button plugin property: "operation" with value: "update"
    Then Enter Value for plugin property table key : "relationTableKey" with values: "string_value"
    Then Select dropdown plugin property: "dedupeBy" with option value: "DESC"
    Then Enter key for plugin property: "dedupeBy" with values: "float_value"
    Then Click plugin property: "updateTableSchema"
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


  @BQ_NULL_MODE_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Validate Successful record  BigQuery source plugin with all NULL values in one column and Few NULL value in different cloumn.
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
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "updateTableSchema"
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
    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table



  @BQ_TIME_STAMP_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify record insert from source BigQuery plugin with partition type Time (Date/timestamp/datetime).
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
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Enter input plugin property: "partitionByField" with value: "partiontion_by_field_value"
    Then Click plugin property: "updateTableSchema"
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
    #Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table


  @BQ_INSERT_SOURCE_TEST @BQ_INSERT_SECOND_SOURCE_TEST @BQ_SINK_TEST
  Scenario: Verify BigQuery With Different Schema RecordName
    Given Open Datafusion Project to configure pipeline
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Select plugin: "BigQuery" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Wrangler" from the plugins list as: "Transform"
    When Select plugin: "Wrangler" from the plugins list as: "Transform"
    Then Move plugins: "Wrangler" by xOffset 250 and yOffset 300
    Then Move plugins: "BigQuery" by xOffset 100 and yOffset 200
    Then Connect plugins: "BigQuery" and "Wrangler2" to establish connection
    Then Connect plugins: "BigQuery2" and "Wrangler" to establish connection
    Then Connect plugins: "Wrangler" and "Wrangler2" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "Wrangler2" and "BigQuery3" to establish connection
    Then Navigate to the properties page of plugin: "BigQuery2"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery2" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    And Enter input plugin property: "referenceName" with value: "Reference"
    And Replace input plugin property: "project" with value: "projectId"
    And Enter input plugin property: "datasetProject" with value: "datasetprojectId"
    And Replace input plugin property: "dataset" with value: "dataset"
    And Enter input plugin property: "table" with value: "bqSourceTable"
    Then Click on the Get Schema button
    Then Validate "BigQuery" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler"
    Then Enter textarea plugin property: "directives" with value: "drop :TableName"
    Then Validate "Wrangler" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Wrangler2"
    Then Enter textarea plugin property: "directives" with value: "rename :EmployeeID :EmpID;"
    Then Validate "Wrangler2" plugin properties
    And Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery3"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "datasetProject" with value: "projectId"
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "updateTableSchema"
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

#    Then Validate the values of records transferred to BQ sink is equal to the values from source BigQuery table


