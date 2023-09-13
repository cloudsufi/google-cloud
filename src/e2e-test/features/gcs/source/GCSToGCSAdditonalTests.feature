@GCS_Source
Feature: GCS source - Verification of GCS to GCS Additional Tests successful

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST @EXISTING_GCS_CONNECTION
  Scenario: To verify the pipeline is getting failed from GCS Source to GCS Sink On Multiple File with connection
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
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "GCS2"
    Then Click plugin property: "switch-useConnection"
    Then Click on the Browse Connections button
    Then Select connection: "gcsConnectionName"
    Then Enter input plugin property: "referenceName" with value: "sinkRef"
    Then Enter GCS sink property path
    Then Select dropdown plugin property: "select-format" with option value: "json"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                  |
      | ERROR | errorMessageMultipleFileWithFirstRowAsHeaderDisabled     |

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS Source to GCS Sink On Multiple File without connection
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Select dropdown plugin property: "select-schema-actions-dropdown" with option value: "clear"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
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
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                     |
      | ERROR | errorMessageMultipleFileWithFirstRowAsHeaderEnabled         |

  @GCS_MULTIPLE_FILES_TEST @GCS_SINK_TEST
  Scenario: To verify the pipeline is getting failed from GCS to GCS when Schema is not cleared in GCS source On Multiple File
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "GCS" from the plugins list as: "Sink"
    Then Connect plugins: "GCS" and "GCS2" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "project" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "sourceRef"
    Then Enter GCS source property path "gcsMultipleFilesPath"
    Then Select GCS property format "delimited"
    Then Enter input plugin property: "delimiter" with value: "delimiterValue"
    Then Toggle GCS source property skip header to true
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
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    And Verify the pipeline status is "Failed"
    Then Open Pipeline logs and verify Log entries having below listed Level and Message:
      | Level | Message                                                      |
      | ERROR | errorMessageMultipleFileWithoutClearDefaultSchema            |
