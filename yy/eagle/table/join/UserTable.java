package yy.eagle.table.join;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import yy.eagle.basic.FileIO;
import yy.eagle.table.info.BasicID;
import yy.eagle.table.info.HiveTable;
import yy.eagle.table.info.SolrField;
import yy.eagle.table.info.UdfFunction;

public class UserTable extends BasicID {

	static {
		KEY = "uid";
		STRUCT = "bigint";
		HIVE_TABLE = "hiido_uf.tw_user_widetable_hive_day";
		SOLR_TABLE = "hiido_uf.tw_user_widetable_solr_day";
	}

	public static void main(String[] args) {
		Map<Integer, HiveTable> map = new HashMap<Integer, HiveTable>();
		Map<String, List<SolrField>> solr = new HashMap<String, List<SolrField>>();
		Map<String, UdfFunction> udfMap = new HashMap<String, UdfFunction>();
		map = MysqlData.getTableMap();
		solr = MysqlData.getSolrField();
		udfMap = MysqlData.getUdfMap();

		// 操作表的ID
		int[] tableId = {1,3,5,6,7,8,12,13,14,15,16,18,19,20};
		//int[] tableId = {17,1,3,8};
		String act = "用户宽表";

		String sqlJoinString = new String();
		sqlJoinString = HiveJoin.getHiveSelect(tableId, map);

		// 输出自定义函数的创建
		StringBuilder func = new StringBuilder();
		for (String udfFunction : SolrField.udfSet) {
			try {
				func.append(udfMap.get(udfFunction).toString()).append("\n");
			} catch (Exception e) {
				System.err.println("找不到函数:" + udfFunction);
				continue;
			}
		}
		Date dt = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		String dateString = sdf.format(dt);

		// Hive建表语句
		FileIO.FileWrite("./file/" + dateString + "_CreateHiveTable.txt",
				TableDDL.createHiveTable(), false);
		// Hive增加新列语句
		FileIO.FileWrite("./file/" + dateString + "_AlterHiveTable.txt",
				TableDDL.alterHiveTable(), false);
		// Hive调度语句
		FileIO.FileWrite("./file/" + dateString + "_InsertHiveTable.txt",
				CONFIG + sqlJoinString, false);

		// Hive输出UDF语句
		FileIO.FileWrite("./file/" + dateString + "_InsertSolrTable.txt",
				func.toString(), false);
		FileIO.FileWrite("./file/" + dateString + "_InsertSolrTable.txt",
				SolrBuild.getSolrSelect(solr.get(act), tableId), true);
		
		// Solr建表语句
		FileIO.FileWrite("./file/" + dateString + "_CreateSolrTable.txt",
				TableDDL.createSolrTable(), false);
		// Solr增加新列语句
		FileIO.FileWrite("./file/" + dateString + "_AlterSolrTable.txt",
				TableDDL.alterSolrTable(), false);

	}

	public void getDate(ResultSet set) throws SQLException {
	}

}
