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

@PubSub_Source
Feature: PubSub Source - Verification of PubSub to PubSub successful data transfer in different formats.

  @PUBSUB_SOURCE_TEST @PUBSUB_SUBSCRIPTION_TEST
  Scenario: Validate successful transfer of records from PubSub(source) to PubSub(sink)  having parquet format in both source and sink plugins.
    Given Open Datafusion Project to configure pipeline
    When Select data pipeline type as: "Data Pipeline - Realtime"
    When Expand Plugin group in the LHS plugins list: "Source"
    When Select plugin: "Pub/Sub" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "Pub/Sub" from the plugins list as: "Sink"
    Then Enter input plugin property: "referenceName" with value: "PubsubReferenceName"
    And Replace input plugin property: "project" with value: "projectId"
    Then Enter input plugin property: "subscription" with value: "pubSubSourceSubscription"
    Then Enter input plugin property: "topic" with value: "pubSubSourceTopic"
