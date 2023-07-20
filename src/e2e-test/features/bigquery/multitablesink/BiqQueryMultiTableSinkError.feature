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
Feature: BigQueryMultiTable sink - Validate BigQueryMultiTable sink plugin error scenarios

  Scenario Outline: Verify BigQueryMultiTable Sink properties validation errors for mandatory fields
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQuery Multi Table
    Then Open BiqQueryMultiTable sink properties
    Then Enter the BigQueryMultiTable properties with blank property "<property>"
    Then Validate mandatory property error for "<property>"
    Examples:
      | property |
      | dataset  |

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of chunk size
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQuery Multi Table
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Enter BiqQueryMultiTable sink property GCS upload request chunk size "bqmtInvalidChunkSize"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "gcsChunkSize"

  Scenario:Verify BQMT Sink properties validation errors for incorrect dataset
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQuery Multi Table
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "bqmtInvalidSinkDataset"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "dataset"

  Scenario:Verify BQMT Sink properties validation errors for incorrect reference name
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQuery Multi Table
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name "bqmtInvalidSinkReferenceName"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "referenceName"

  Scenario:Verify BQMT Sink properties validation errors for incorrect value of temporary bucket name
    Given Open Datafusion Project to configure pipeline
    When Sink is BiqQuery Multi Table
    Then Open the BiqQueryMultiTable sink properties
    Then Enter BiqQueryMultiTable sink property projectId "projectId"
    Then Override Service account details if set in environment variables
    Then Enter BiqQueryMultiTable sink property reference name
    Then Enter BiqQueryMultiTable sink property dataset "dataset"
    Then Enter BiqQueryMultiTable sink property temporary bucket name "bqmtInvalidTemporaryBucket"
    Then Verify the BiqQueryMultiTable sink validation error message for invalid property "bucket"