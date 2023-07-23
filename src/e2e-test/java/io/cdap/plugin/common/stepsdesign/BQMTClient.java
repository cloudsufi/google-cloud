package io.cdap.plugin.common.stepsdesign;

import com.google.common.base.Strings;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BQMTClient {

  private static final String database = PluginPropertyUtils.pluginProp("databaseName");

  public static Connection getMysqlConnection() throws SQLException, ClassNotFoundException {
    Class.forName("com.mysql.cj.jdbc.Driver");
    return DriverManager.getConnection("jdbc:mysql://" + System.getenv("MYSQL_HOST") + ":" +
                                         System.getenv("MYSQL_PORT") + "/" + database + "?tinyInt1isBit=false",
                                       System.getenv("MYSQL_USERNAME"), System.getenv("MYSQL_PASSWORD"));
  }

  public static void createSourceDatatypesTable(String sourceTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createSourceTableQuery = "CREATE TABLE " + sourceTable + " " + datatypesColumns;
      statement.executeUpdate(createSourceTableQuery);

      // Insert dummy data.
      int rowCount = 1;
      while (!Strings.isNullOrEmpty(PluginPropertyUtils.pluginProp("datatypesValue" + rowCount))) {
        String datatypesValues = PluginPropertyUtils.pluginProp("datatypesValue" + rowCount);
        String datatypesColumnsList = PluginPropertyUtils.pluginProp("datatypesColumnsList");
        statement.executeUpdate("INSERT INTO " + sourceTable + " " + datatypesColumnsList + " " + datatypesValues);
        rowCount++;
      }
    }
  }

  public static void createTargetDatatypesTable(String targetTable) throws SQLException, ClassNotFoundException {
    try (Connection connect = getMysqlConnection();
         Statement statement = connect.createStatement()) {
      String datatypesColumns = PluginPropertyUtils.pluginProp("datatypesColumns");
      String createTargetTableQuery = "CREATE TABLE " + targetTable + " " + datatypesColumns;
      statement.executeUpdate(createTargetTableQuery);
    }
  }
}
