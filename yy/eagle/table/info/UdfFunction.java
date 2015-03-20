package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;

public class UdfFunction {
	public int id;
	public String path;
	public String name;
	
	public UdfFunction(ResultSet set){
		try {
			id = set.getInt("id");
			path = set.getString("path");
			name = set.getString("name");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public String toString(){
		return "CREATE TEMPORARY FUNCTION " + name + " AS '" + path + "';";
	}
}
