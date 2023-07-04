/*
 * Copyright © 2023 Cask Data, Inc.
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
package io.cdap.plugin.common.stepsdesign;

import com.google.api.gax.paging.Page;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.storage.*;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.cdap.e2e.pages.actions.CdfPipelineRunAction;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import io.cucumber.java.en.Then;
import org.junit.Assert;
import stepsdesign.BeforeActions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.google.gson.JsonObject;
import java.util.Map;

public class GCSValidationHelper {
  private static final String PROJECT_ID = PluginPropertyUtils.pluginProp("projectId");
  private static final String BUCKET_NAME = TestSetupHooks.gcsTargetBucketName;
  private static final String expectedBQTable = TestSetupHooks.bqSourceTable;
  //  private static final List<Object> bigQueryRows= new ArrayList<>();
  private static final Gson gson = new Gson();

  public static void main(String[] args) throws IOException, InterruptedException {
    String gcsbucket = "testsamplegcs";
    String bqSourceTable = "E2E_SOURCE_268e63e3_5444_4f1a_8f4f_35f33644ee53";
    validateActualDataToExpectedData(bqSourceTable, gcsbucket);
  }

  public static boolean validateActualDataToExpectedData(String table, String bucketName) throws IOException, InterruptedException {
    Map<String, JsonObject> expectedData = new HashMap<>();
    Map<String, JsonObject> actualData = new HashMap<>();
    getBigQueryTableData(table, expectedData);

    boolean isMatched = matchJsonList(table, bucketName);
    if (isMatched) {
      System.out.println("The lists are matched.");
    } else {
      System.out.println("The lists are not matched.");
    }
    return isMatched;
  }

  public boolean matchData(String table, String bucketName) throws IOException, InterruptedException {
    Map<String, JsonObject> bigQueryMap = new HashMap<>();
    Map<String, JsonObject> fileMap = listBucketObjects(bucketName);

    getBigQueryTableData(table, bigQueryMap);

    boolean isMatched = matchJsonMaps(expectedData, actualData);

    if (isMatched) {
      System.out.println("The data is matched.");
    } else {
      System.out.println("The data is not matched.");
      System.out.println("Mismatched data:");
      printMismatchedData(expectedData, actualData);
    }
    return isMatched;
  }


//  public static boolean matchJsonList(String table, String bucketName) {
//    try {
////      List<List<String>> expectedData = readDataFromFile(expectedTable);
//      Map<String, JsonObject> bigQueryRows = new HashMap<>();
//      List<Object> expectedData = getBigQueryTableData(table, bigQueryRows);
//
//      // Compare data from GCS with expectedSourceTable
//      List<List<String>> actualData = listBucketObjects(bucketName);
//      List<String> demo = new ArrayList<>();
//      for (int i = 0; i < actualData.get(0).size(); i++) {
//        demo.add(actualData.get(0).get(i));
//      }
//      int actualRowCount = demo.size();
//      int expectedRowCount = expectedData.size();
//
//      if (actualRowCount != expectedRowCount) {
//        System.out.println("Row count does not match.");
//      }
//
//      for (int i = 0; i < expectedRowCount; i++) {
//        List<Object> expectedRow = Collections.singletonList(expectedData.get(i));
//        List<String> actualRow = Collections.singletonList(demo.get(i));
//
//        if (!expectedRow.equals(actualRow)) {
//          return false;
//        }
//      }
//    } catch (Exception e) {
//      e.printStackTrace();
//    }
//    return true;
//  }

  public static Map<String, JsonObject> listBucketObjects(String bucketName) {
    List<List<String>> fileData = new ArrayList<>();
    Storage storage = StorageOptions.newBuilder().setProjectId(PROJECT_ID).build().getService();
    Page<Blob> blobs = storage.list(bucketName);

    // Adding all the Objects which have data in a list.
    List<Blob> bucketObjects = StreamSupport.stream(blobs.iterateAll().spliterator(), true)
      .filter(blob -> blob.getSize() != 0)
      .collect(Collectors.toList());

    Stream<String> objectNamesWithData = bucketObjects.stream().map(blob -> blob.getName());
    List<String> bucketObjectNames = objectNamesWithData.collect(Collectors.toList());

    // Add the 0th index to the fileData list
    if (!bucketObjectNames.isEmpty()) {
      String objectName = bucketObjectNames.get(0);
      if (objectName.contains("part-r")) {
        Map<String, JsonObject> objectData = fetchObjectData(PROJECT_ID, bucketName, objectName);
        fileData.add(objectData);
      }
    }

    return fileData;
  }

  private static String[] fetchObjectData(String projectId, String bucketName, String objectName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    byte[] objectData = storage.readAllBytes(bucketName, objectName);
    String objectDataAsString = new String(objectData, StandardCharsets.UTF_8);

    // Splitting using the delimiter as a File can have more than one record.
    return objectDataAsString.split("\n");
  }

  /**
   * Retrieves the data from a specified BigQuery table and populates it into the provided list of objects.
   *  @param table        The name of the BigQuery table to fetch data from.
   * @param bigQueryRows The list to store the fetched BigQuery data.
   * @return
   */

  private static void getBigQueryTableData(String table, Map<String, JsonObject> bigQueryRows)
    throws IOException, InterruptedException {

    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);
//    result.iterateAll().forEach(value -> bigQueryRows.add(value.get(0).getValue()));
    for (FieldValueList row : result.iterateAll()) {
      JsonObject json = gson.fromJson(row.get(0).getStringValue(), JsonObject.class);
      JsonElement idElement = json.get("ID");
      if (idElement != null && idElement.isJsonPrimitive()) {
        String id = idElement.getAsString(); // Replace "id" with the actual key in the JSON
        bigQueryRows.put(id, json);
      } else {
        System.out.println("Data Mismatched");
      }
    }
    System.out.println(bigQueryRows);
  }

  }

  @Then("Validate the values of records transferred to GCS bucket is equal to the values from source BigQuery table")
  public void validateTheValuesOfRecordsTransferredToGcsBucketIsEqualToTheValuesFromSourceBigQueryTable()
    throws InterruptedException, IOException {
    int sourceBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqSourceTable"));
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + sourceBQRecordsCount);
    Assert.assertEquals("Out records should match with GCS file records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), sourceBQRecordsCount);

    boolean recordsMatched = GCSValidationHelper.validateActualDataToExpectedData(
      TestSetupHooks.bqSourceTable, TestSetupHooks.gcsTargetBucketName);
    Assert.assertTrue("Value of records transferred to the GCS bucket file should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }

  @Then("Validate the values of records transferred from GCS bucket file is equal to the values of target BigQuery table")
  public void validateTheValuesOfRecordsTransferredFromGcsBucketFileIsEqualToTheValuesOfTargetBigQueryTable()
    throws InterruptedException, IOException {
    int targetBQRecordsCount = BigQueryClient.countBqQuery(PluginPropertyUtils.pluginProp("bqTargetTable"));
    BeforeActions.scenario.write("No of Records from source BigQuery table:" + targetBQRecordsCount);
    Assert.assertEquals("Out records should match with GCS file records count",
                        CdfPipelineRunAction.getCountDisplayedOnSourcePluginAsRecordsOut(), targetBQRecordsCount);

    boolean recordsMatched = GCSValidationHelper.validateActualDataToExpectedData(
      TestSetupHooks.gcsTargetBucketName, TestSetupHooks.bqTargetTable);
    Assert.assertTrue("Value of records transferred to the GCS bucket file should be equal to the value " +
                        "of the records in the source table", recordsMatched);
  }
}
