@Spanner_Source @SPANNER_TEST
Feature: Spanner source - Verification of Spanner to Spanner successful data transfer with macro arguments

  @SPANNER_SINK_TEST @SPANNER_TEST @Spanner_Source_Required
  Scenario: To verify data is getting transferred from Spanner to Spanner with macro arguments
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "Spanner" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Spanner" from the plugins list as: "Sink"
    Then Connect plugins: "Spanner" and "Spanner2" to establish connection
    Then Navigate to the properties page of plugin: "Spanner"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Click on the Macro button of Property: "instanceId" and set the value to: "spannerInstance"
    Then Click on the Macro button of Property: "databaseName" and set the value to: "spannerDatabase"
    Then Click on the Macro button of Property: "tableName" and set the value to: "spannerSourceTable"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "Spanner2"
    Then Enter Spanner property reference name
    Then Enter Spanner property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Click on the Macro button of Property: "instanceId" and set the value to: "spannerInstance"
    Then Click on the Macro button of Property: "databaseName" and set the value to: "spannerTargetDatabase"
    Then Click on the Macro button of Property: "tableName" and set the value to: "spannerTargetTable"
    Then Enter Spanner sink property primary key "spannerSinkPrimaryKeySpanner"
    Then Validate "Spanner" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Enter runtime argument value "spannerInstance" for key "spannerInstance"
    Then Enter runtime argument value "spannerDatabase" for key "spannerDatabase"
    Then Enter runtime argument value "spannerSourceTable" for key "spannerSourceTable"
    Then Enter runtime argument value "spannerTargetDatabase" for key "spannerTargetDatabase"
    Then Enter runtime argument value "spannerTargetTable" for key "spannerTargetTable"
    Then Run the preview of pipeline with runtime arguments
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Enter runtime argument value "spannerInstance" for key "spannerInstance"
    Then Enter runtime argument value "spannerDatabase" for key "spannerDatabase"
    Then Enter runtime argument value "spannerSourceTable" for key "spannerSourceTable"
    Then Enter runtime argument value "spannerTargetDatabase" for key "spannerTargetDatabase"
    Then Enter runtime argument value "spannerTargetTable" for key "spannerTargetTable"
    Then Run the Pipeline in Runtime with runtime arguments
    Then Wait till pipeline is in running state
    Then Verify the pipeline status is "Succeeded"
    Then Validate records transferred to target spanner table with record counts of source spanner table