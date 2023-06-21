@GCS_Sink
Feature: GCS sink - Verify GCS Sink plugin error scenarios

  Scenario Outline:Verify GCS Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Enter the GCS properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property        |
      | path            |
      | format          |

  @GCS_SINK_TEST
  Scenario:Verify GCS Sink properties validation errors for invalid reference name
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
#    Then Override Service account details if set in environment variables
    Then Select radio button plugin property: "serviceAccountType" with value: "JSON"
    Then Read Credentials file "serviceAccountJSON" with value: "keys"
    Then Enter input plugin property: "referenceName" with value: "gcsInvalidRefName"
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "referenceName" is displaying an in-line error message: "errorMessageInvalidReferenceName"

  @GCS_SINK_TEST
  Scenario:Verify GCS Sink properties validation errors for invalid file system properties
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
    Then Select radio button plugin property: "serviceAccountType" with value: "JSON"
    Then Read Credentials file "serviceAccountJSON" with value: "keys"
    Then Enter textarea plugin property: "fileSystemProperties" with value: "gcsInvalidFileSysProperty"
    Then Enter input plugin property: "referenceName" with value: "gcsReferenceName"
    Then Enter GCS sink property path
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "fileSystemProperties" is displaying an in-line error message: "errorMessageIncorrectFileSystemProperties"

  @GCS_SINK_TEST
  Scenario:Verify GCS Sink properties validation errors for invalid bucket path
    Given Open Datafusion Project to configure pipeline
    When Sink is GCS
    Then Open GCS sink properties
    Then Replace input plugin property: "project" with value: "projectId"
    Then Select radio button plugin property: "serviceAccountType" with value: "JSON"
    Then Read Credentials file "serviceAccountJSON" with value: "keys"
    Then Enter input plugin property: "referenceName" with value: "gcsReferenceName"
    Then Enter GCS source property path "gcsInvalidBucketName"
    Then Select GCS property format "csv"
    Then Click on the Validate button
    Then Verify that the Plugin Property: "path" is displaying an in-line error message: "errorMessageInvalidBucketName"






