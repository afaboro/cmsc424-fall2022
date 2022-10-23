import java.sql.*;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringJoiner;

public class MetaData {
	static String dataTypeName(int i) {
		switch (i) {
			case java.sql.Types.INTEGER: return "Integer";
			case java.sql.Types.REAL: return "Real";
			case java.sql.Types.VARCHAR: return "Varchar";
			case java.sql.Types.TIMESTAMP: return "Timestamp";
			case java.sql.Types.DATE: return "Date";
		}
		return "Other";
	}
	
  public static void executeMetadata(String databaseName) {
		/************* 
		 * Add you code to connect to the database and print out the metadta for the database databaseName. 
		 ************/

		Connection connection = null;
		
    try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + databaseName,"root", "root");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

    try {
      DatabaseMetaData mData = connection.getMetaData();
      ResultSet rs = mData.getTables(null, null, null, new String[]{"TABLE"});

      ArrayList<String> tables = new ArrayList<>();
      
      System.out.println("### Tables in the Database");
      
      while (rs.next()) {
        String tableName = rs.getString("TABLE_NAME");
        tables.add(tableName.toUpperCase());

        System.out.println("-- Table " + tableName.toUpperCase());

        ResultSet columns = mData.getColumns(null, null, tableName, null);
        Map<String, String> tableAttributes = new TreeMap<>();
        
        while (columns.next()) {
          String columnName = columns.getString("COLUMN_NAME");
          int dType = Integer.parseInt(columns.getString("DATA_TYPE"));

          String dataType = dataTypeName(dType);
          tableAttributes.put(columnName.toUpperCase(), dataType);
        }
        
        System.out.print("Attributes: ");
        StringJoiner join = new StringJoiner(", ");
        tableAttributes.forEach((key, value) -> join.add(key + " (" + value + ")"));
        System.out.println(join.toString());

        ResultSet primaryKeys = mData.getPrimaryKeys(null, null, tableName);
        ArrayList<String> pKeys = new ArrayList<>();

        while (primaryKeys.next()) {
          String pKey = primaryKeys.getString("COLUMN_NAME").toUpperCase();
          pKeys.add(pKey);
        }
        
        

        if (pKeys.size() > 0) {
          Collections.sort(pKeys);
          StringJoiner joiner = new StringJoiner(", ");
          pKeys.forEach((primary) -> joiner.add(primary));
          System.out.print("Primary Key: ");
          System.out.println(joiner.toString());
        } else {
          System.out.println("Primary Key:");
        }
      }

      System.out.println();
      System.out.println("### Joinable Pairs of Tables (based on Foreign Keys)");

      for (String table: tables) {
        ResultSet foreignKeys = mData.getExportedKeys(null, null, table.toLowerCase());

        while (foreignKeys.next()) {
          String pkTableName = foreignKeys.getString("PKTABLE_NAME").toUpperCase();
          String fkTableName = foreignKeys.getString("FKTABLE_NAME").toUpperCase();
          String pkColName = foreignKeys.getString("PKCOLUMN_NAME").toUpperCase();
          String fkColName = foreignKeys.getString("FKCOLUMN_NAME").toUpperCase();

          System.out.println(pkTableName + " can be joined " + fkTableName + " on attributes " + pkColName + " and " + fkColName);
        }
      }
    } catch (SQLException e) {
      System.out.print(e);
      return;
    }
	}

	public static void main(String[] argv) {
		executeMetadata(argv[0]);
	}
}
