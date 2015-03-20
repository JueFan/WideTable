package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HiveField extends BasicID {
	public String status;
	public int table_id;
	public String fname;	//字段名称
	public String struct;
	public String comment;
	public String new_fname;	//大宽表的字段名称（=t+table_id+_+fname）
	public static List<HiveField> fields = new ArrayList<HiveField>();
	
	@Override
	public void getDate(ResultSet set) throws SQLException {
		id = set.getInt("id");
		table_id = set.getInt("table_id");
		status = new String();
		fname = new String();
		comment = new String();
		struct = new String();
		status = set.getString("status");
		fname = set.getString("fname");
		struct = set.getString("struct");
		comment = set.getString("comment");
		new_fname = new String();
		new_fname = "t" + Integer.toString(table_id)
				+ "_" + fname;
	}
	
	public  String combine(){
		String tmp = new String();
		tmp = ", " + fname + " AS " + new_fname;
		HiveField.fields.add(this);
		return tmp;
	}

}
