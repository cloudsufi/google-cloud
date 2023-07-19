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

@BigQuery_Sink
Feature: BigQuery sink - Validate BigQuery sink plugin error scenarios

  Scenario Outline:Verify BigQuery Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter the BigQuery properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property      |
      | dataset       |
      | table         |

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect value of chunk size
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BigQuery sink property GCS upload request chunk size "bqInvalidChunkSize"
    Then Verify the BigQuery validation error message for invalid property "gcsChunkSize"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect dataset
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "bqInvalidSinkDataset"
    Then Enter BigQuery sink property table name
    Then Verify the BigQuery validation error message for invalid property "dataset"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect table
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery property table "bqInvalidSinkTable"
    Then Verify the BigQuery validation error message for invalid property "table"

  @BQ_SINK_TEST
  Scenario:Verify BigQuery Sink properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Sink is BigQuery
    Then Open BigQuery sink properties
    Then Enter BigQuery property reference name
    Then Enter BigQuery property projectId "projectId"
    Then Enter BigQuery property datasetProjectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BigQuery property dataset "dataset"
    Then Enter BigQuery sink property table name
    Then Enter BigQuery property temporary bucket name "bqInvalidTemporaryBucket"
    Then Verify the BigQuery validation error message for invalid property "bucket"
