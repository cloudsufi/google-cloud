/*
 * Copyright © 2021 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.plugin.gcp.dataplex.common.model;

import java.util.List;

/**
 * The type Schema.
 */
public class Schema {

  private List<Field> fields;
  private List<Field> partitionFields;
  private String partitionStyle;

  public List<Field> getFields() {
    return fields;
  }

  public void setFields(List<Field> fields) {
    this.fields = fields;
  }

  public List<Field> getPartitionFields() {
    return partitionFields;
  }

  public void setPartitionFields(List<Field> partitionFields) {
    this.partitionFields = partitionFields;
  }

  public String getPartitionStyle() {
    return partitionStyle;
  }

  public void setPartitionStyle(String partitionStyle) {
    this.partitionStyle = partitionStyle;
  }
}