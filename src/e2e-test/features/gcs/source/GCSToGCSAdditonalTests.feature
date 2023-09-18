@GCS_Source
Feature: GCS source - Verification of GCS to GCS Additional Tests successful

  @GCS_MULTIPLE_FILES_REGEX_TEST @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify the pipeline is getting failed from GCS to GCS On Multiple File with filter regex using connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesFilterRegexPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "fileRegex" with value: "fileRegexValue"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate the data from GCS Source to GCS Sink with expected json file and target data in GCS bucket

  @GCS_MULTIPLE_FILES_REGEX_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS to GCS On Multiple File with filter regex without using connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter GCS source property path "gcsMultipleFilesFilterRegexPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
    Then Enter input plugin property: "fileRegex" with value: "fileRegexValue"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
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
    Then Validate the data from GCS Source to GCS Sink with expected json file and target data in GCS bucket
