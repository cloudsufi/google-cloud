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
Feature: BigQuery source - Validate BigQuery source plugin error scenarios

  Scenario Outline:Verify BigQuery Source properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery source properties
    Then Enter the BigQuery properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | dataset       |
      | table         |

  @BQ_SOURCE_TEST
  Scenario Outline:Verify BigQuery Source properties validation errors for incorrect values
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Override Service account details if set in environment variables
    Then Enter the BigQuery source properties with incorrect property "<property>" value "<value>"
    Then Validate BigQuery source incorrect property error for table "<property>" value "<value>"
    Examples:
      | property       | value                       |
      | dataset        | bqIncorrectDataset          |
      | table          | bqIncorrectTableName        |
      | datasetProject | bqIncorrectDatasetProjectId |

  Scenario Outline:Verify BigQuery Source properties validation errors for incorrect format of projectIds
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "<ProjectID>"
    Then Enter BigQuery property datasetProjectId "<DatasetProjectID>"
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Verify plugin properties validation fails with 1 error
    Examples:
      | ProjectID                  | DatasetProjectID                  |
      | bqIncorrectFormatProjectId | projectId                         |
      | projectId                  | bqIncorrectFormatDatasetProjectId |

  @BQ_SOURCE_TEST
  Scenario:Verify BigQuery Source properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Source is BigQuery
    Then Open BigQuery source properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery source property table name
    Then Enter BigQuery property temporary bucket name "bqInvalidTemporaryBucket"
    Then Verify the BigQuery validation error message for invalid property "bucket"
