package yy.eagle.table.join;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;

import yy.eagle.table.info.BasicID;
import yy.eagle.table.info.HiveField;
import yy.eagle.table.info.SolrField;

public class TableDDL extends BasicID {

	/**
	 * Hive中间宽表的建表语句
	 */
	public static String createHiveTable() {
		StringBuilder builder = new StringBuilder();
		builder.append("DROP TABLE ");
		builder.append(HIVE_TABLE);
		builder.append(";\nCREATE TABLE ");
		builder.append(HIVE_TABLE);
		builder.append("(").append(KEY).append(" ");
		builder.append(STRUCT); // 宽表的关键字段类型
		builder.append(",\n");
		Collections.sort(HiveField.fields, compare);	//按Mysql的自增ID对字段排序
		int i = 0;
		for (; i < HiveField.fields.size() - 1; i++) {
			builder.append(HiveField.fields.get(i).new_fname + "  "
					+ HiveField.fields.get(i).struct + ",\n");
		}
		builder.append(HiveField.fields.get(i).new_fname + "  "
				+ HiveField.fields.get(i).struct + "\n");
		builder.append(")\nPARTITIONED BY (dt STRING)\nSTORED AS RCFILE;");
		System.out.println("<==========Hive建表语句生成：成功==========>");
		return builder.toString();
	}

	public static String alterHiveTable() {
		StringBuilder builder = new StringBuilder();
		Collections.sort(HiveField.fields, compare);
		for (HiveField field : HiveField.fields) {
			if (field.status.equals("new")) {
				builder.append("ALTER TABLE ");
				builder.append(HIVE_TABLE);
				builder.append(" ADD COLUMNS (");
				builder.append(field.new_fname).append("  ")
						.append(field.struct).append(");\n");
			}
		}
		System.out.println("<==========Hive增列语句生成：成功==========>");
		return builder.toString();
	}

	public static String createSolrTable() {
		StringBuilder builder = new StringBuilder();
		builder.append("DROP TABLE ");
		builder.append(SOLR_TABLE);
		builder.append(";\nCREATE TABLE ");
		builder.append(SOLR_TABLE);
		builder.append("(").append(KEY).append(" ");
		builder.append(STRUCT); // 宽表的关键字段类型
		builder.append(",\n");
		Collections.sort(SolrField.fields, compare);
		int i = 0;
		for (; i < SolrField.fields.size() - 1; i++) {
			builder.append(SolrField.fields.get(i).fname + "  "
					+ SolrField.fields.get(i).struct + ",\n");
		}
		builder.append(SolrField.fields.get(i).fname + "  "
				+ SolrField.fields.get(i).struct + "\n");
		builder.append(")\nPARTITIONED BY (dt STRING)\nROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe'\n"
				+ "WITH SERDEPROPERTIES ('serialization.null.format'='')\nSTORED AS RCFILE;");
		System.out.println("<==========Solr建表语句生成：成功==========>");
		return builder.toString();
	}
	
	public static String alterSolrTable() {
		StringBuilder builder = new StringBuilder();
		Collections.sort(SolrField.fields, compare);
		for (SolrField field : SolrField.fields) {
				builder.append("ALTER TABLE ");
				builder.append(SOLR_TABLE);
				builder.append(" ADD COLUMNS (");
				builder.append(field.fname).append("  ")
						.append(field.struct).append(");\n");
		}
		System.out.println("<==========Solr增列语句生成：成功==========>");
		return builder.toString();
	}
	
	@Override
	public void getDate(ResultSet set) throws SQLException {

	}

}
