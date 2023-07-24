package io.cdap.plugin.common.stepsdesign;

import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableResult;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.cdap.e2e.utils.BigQueryClient;
import io.cdap.e2e.utils.PluginPropertyUtils;
import org.junit.Assert;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

public class BQValidation {
  public static void main(String[] args) throws IOException, InterruptedException {
    validateSourceBQToTargetBQRecord("BQMTtest", "TableBQ");
  }

  private static List<JsonObject> getBigQueryTableData(String table)
    throws IOException, InterruptedException {
    List<JsonObject> bigQueryRows = new ArrayList<>();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    String selectQuery = "SELECT TO_JSON(t) FROM `" + projectId + "." + dataset + "." + table + "` AS t";
    TableResult result = BigQueryClient.getQueryResult(selectQuery);

//    result.iterateAll().forEach(value -> bigQueryRows.add((JsonObject) value.get(0).getValue()));
    result.iterateAll().forEach(value -> {
      String json = value.get(0).getStringValue();
      JsonObject jsonObject = new JsonParser().parse(json).getAsJsonObject();
      bigQueryRows.add(jsonObject);
    });

    return bigQueryRows;
  }

  public static boolean validateSourceBQToTargetBQRecord(String sourceTable, String targetTable)
    throws IOException, InterruptedException {
    List<JsonObject> bigQuerySourceResponse = getBigQueryTableData(sourceTable);
    List<JsonObject> bigQueryTargetResponse = getBigQueryTableData(targetTable);

    // Compare the data from the source and target BigQuery tables
    return compareJsonDataWithJsonData(bigQuerySourceResponse, bigQueryTargetResponse, targetTable);
  }

  public static boolean compareJsonDataWithJsonData(List<JsonObject> sourceData, List<JsonObject> targetData, String tableName) {
    if (targetData == null) {
      Assert.fail("targetData is null");
      return false;
    }

    int columnCountSource = 0;
    if (!sourceData.isEmpty()) {
      columnCountSource = sourceData.get(0).entrySet().size();
    }

    int columnCountTarget = 0;
    if (!targetData.isEmpty()) {
      columnCountTarget = targetData.get(0).entrySet().size();
    }
    com.google.cloud.bigquery.BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    //    BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
    String projectId = PluginPropertyUtils.pluginProp("projectId");
    String dataset = PluginPropertyUtils.pluginProp("dataset");
    // Build the table reference
    TableId tableRef = TableId.of(projectId, dataset, tableName);
    // Get the table schema
    Schema schema = bigQuery.getTable(tableRef).getDefinition().getSchema();
    // Iterate over the fields
    int currentColumnCount = 1;
    while (currentColumnCount <= columnCountSource) {
      for (Field field : schema.getFields()) {
        String columnName = field.getName();
        String columnType = field.getType().toString();

        // Compare the number of columns in the source and target
        Assert.assertEquals(columnCountSource, columnCountTarget);

        switch (columnType){
          case "STRING":
            break;
          case "ID":
            break;
        }

      }
    }
    return true;
  }
}

