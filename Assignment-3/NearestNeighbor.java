import java.sql.*;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.Arrays;

public class NearestNeighbor 
{
	static double jaccard(HashSet<String> s1, HashSet<String> s2) {
		int total_size = s1.size() + s2.size();
		int i_size = 0;
		for (String s: s1) {
			if (s2.contains(s))
				i_size++;
		}
		return ((double) i_size)/(total_size - i_size);
	}

  static void alterTable(Connection connection, Statement stmt) {
		String alterQuery = "alter table users add column nearest_neighbor integer;";
		
    try {
			stmt = connection.createStatement();
			stmt.executeQuery(alterQuery);
			stmt.close();
		} catch (SQLException e) {
			System.out.println(e);
		}
  }

  static TreeMap<Integer, HashSet<String>> getUserTags(Connection connection, Statement stmt) {
    String dataQuery = "select users.id, array_remove(array_agg(posts.tags), null) as arr " +
                       "from users, posts " + 
                       "where users.id = posts.OwnerUserId and users.id < 5000 " +
                       "group by users.id " + 
                       "having count(posts.tags) > 0;";

    TreeMap<Integer, HashSet<String>> users = new TreeMap<>();
    
    try {
      stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(dataQuery);
			
      while (rs.next()) {
        String userId = rs.getString("id");
        String tagString = rs.getString("arr");
        
        ArrayList<String> tagList = new ArrayList<>(Arrays.asList(tagString.split("[{<>,}]+")));
        tagList.remove(0);
        
        HashSet<String> tags = new HashSet<>(tagList);
        users.put(Integer.parseInt(userId), tags);
			}
      
      stmt.close();
      return users;
    } catch (SQLException e) {
      System.out.println(e);
      return null;
    }
  }

  static void addNearestNeighbor(Connection connection, Statement stmt, int userId, int neighbor) {
    String insertQuery = "update users set nearest_neighbor = '" + neighbor + "' where id = '" + userId + "';";
    try {
      stmt = connection.createStatement();
      stmt.executeUpdate(insertQuery);
      stmt.close();
    } catch (SQLException e) {
      System.out.println(e);
    }
  }

	public static void executeNearestNeighbor() {
		/************* 
		 * Add your code to add a new column to the users table (set to null by default), calculate the nearest neighbor for each node (within first 5000), and write it back into the database for those users..
		 ************/
		Connection connection = null;
		try {
			connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/stackexchange","root", "root");
		} catch (SQLException e) {
			System.out.println("Connection Failed! Check output console");
			e.printStackTrace();
			return;
		}

		Statement stmt = null;
    alterTable(connection, stmt);
    TreeMap<Integer, HashSet<String>> usersTags = getUserTags(connection, stmt);
    // for (Integer u: usersTags.keySet()) {
    //   System.out.println(u + ": " + usersTags.get(u));
    // }

    if (usersTags != null) {
      for (Integer u1: usersTags.keySet()) {
        HashSet<String> t1 = usersTags.get(u1);
        double similarity = 0.0;
        Integer nearest = 0;
        
        for (Integer u2: usersTags.keySet()) {
          if (!u1.equals(u2)) {
            HashSet<String> t2 = usersTags.get(u2);
            double temp = jaccard(t1, t2);
            if (similarity < temp) {
              similarity = temp;
              nearest = u2;
            } else if (similarity == temp) {
              if (nearest > u2) {
                nearest = u2;
              }
            }
          }
        }
        addNearestNeighbor(connection, stmt, u1, nearest);
      }
    }
    
    try {
      connection.close();
    } catch (SQLException e) {
      System.out.println(e);
    }
	}

	public static void main(String[] argv) {
		executeNearestNeighbor();
	}
}
