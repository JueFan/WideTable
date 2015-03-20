package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class HiveTable extends BasicID{
	public String table_name;
	public String comment;
	public String condition;
	public List<HiveField> fields = new ArrayList<HiveField>();
	public String tmp_table;
	@Override
	public void getDate(ResultSet set) throws SQLException {
		id = set.getInt("id");
		table_name = new String();
		comment = new String();
		condition = new String();
		tmp_table = new String();
		table_name = set.getString("table_name");
		comment = set.getString("comment");
		condition = set.getString("condition");
		tmp_table = "tb" + Integer.toString(id);
	}
	
	public  String getSelect(){
		StringBuilder builder = new StringBuilder();
		builder.append(" (SELECT ").append(KEY);
		Collections.sort(fields, compare);
		for(HiveField field: fields){
			builder.append(field.combine());
		}
		builder.append(" FROM ");
		builder.append(table_name);
		builder.append(" WHERE ");
		builder.append(condition);
		builder.append(")");
		builder.append("tb").append(Integer.toString(id));
		return builder.toString();
	}
}
