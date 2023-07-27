package io.cdap.plugin.common.stepsdesign;

import com.google.common.base.Strings;
import io.cdap.e2e.utils.PluginPropertyUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class BigQueryClient {

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
      // Insert row1 data
      String datatypesValues = PluginPropertyUtils.pluginProp("datatypesValueBQMT1");
      String datatypesColumnsList = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate(
        "INSERT INTO " + "." + sourceTable + " " + datatypesColumnsList + " " + datatypesValues);
// Insert row2 data.
      String datatypesValues2 = PluginPropertyUtils.pluginProp("datatypesValueBQMT2");
      String datatypesColumnsList2 = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate(
        "INSERT INTO " + "." + sourceTable + " " + datatypesColumnsList2 + " " + datatypesValues2);

      String datatypesValues3 = PluginPropertyUtils.pluginProp("datatypesValueBQMT3");
      String datatypesColumnsList3 = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate(
        "INSERT INTO " + "." + sourceTable + " " + datatypesColumnsList3 + " " + datatypesValues3);

      String datatypesValues4 = PluginPropertyUtils.pluginProp("datatypesValueBQMT4");
      String datatypesColumnsList4 = PluginPropertyUtils.pluginProp("datatypesColumnsList");
      statement.executeUpdate(
        "INSERT INTO " + "." + sourceTable + " " + datatypesColumnsList4 + " " + datatypesValues4);
    }
  }
}
