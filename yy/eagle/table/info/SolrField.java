package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SolrField extends BasicID {
	public String act;
	public int table_id;
	public String fname;
	public static Set<String> udfSet = new HashSet<String>();
	public String compose;
	public String struct;
	public String comment;
	public static List<SolrField> fields = new ArrayList<SolrField>();
	@Override
	public void getDate(ResultSet set) throws SQLException {
		id = set.getInt("id");
		act = new String();
		fname = new String();
		compose = new String();
		struct = new String();
		comment = new String();
		act = set.getString("act");
		table_id = set.getInt("table_id");
		fname = set.getString("fname");
		if (set.getString("udf") != null) {
			for(String udf:set.getString("udf").split(";") )
			udfSet.add(udf);
		}
		compose = set.getString("compose");
		struct = set.getString("struct");
		comment = set.getString("comment");
	}
	
	public String combine(){
		StringBuilder builder = new StringBuilder();
		builder.append(",");
		builder.append((compose.startsWith("\\"))? compose.substring(1, compose.length()): compose);
		builder.append(" AS ");
		builder.append(fname);
		fields.add(this);
		return builder.toString();	
	}
}
