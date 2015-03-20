package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;

public class MProduct extends BasicID{
	public String name;
	public String product_platform;
	public String product_key;
	public String ver;
	public String from;
	
	@Override
	public void getDate(ResultSet set) throws SQLException {
		name = new String();
		product_platform = new String();
		product_key = new String();
		ver = new String();
		from = new String();
		id = set.getInt("id");
		name = set.getString("name");
		product_platform = set.getString("product_platform");
		product_key = set.getString("product_key");	
		ver = "ver_" + Integer.toString(id);
		from = "from_" + Integer.toString(id);
	}

}
