package yy.eagle.table.join;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import yy.eagle.table.info.BasicID;
import yy.eagle.table.info.HiveField;
import yy.eagle.table.info.HiveTable;

public class HiveJoin extends BasicID{
	
	public static int joinID = 0; //外层临时表ID

	/**
	 * 俩表进行左外链接
	 * @param t1 当前驱动表
	 * @param t2 右表
	 * @param q1 驱动表的子查询
	 * @param q2 右表的子查询
	 * @return
	 */
	public static String getJoinSql(HiveTable t1, HiveTable t2, String q1, String q2){
		StringBuilder builder = new StringBuilder();
		builder.append("( SELECT ");
		builder.append("tb").append(Integer.toString(t1.id)).append(".*");
		//抽取出后置表的字段
		for(HiveField field: t2.fields){
			builder.append(" ,").append(field.new_fname);
		}
		builder.append(" FROM ");
		builder.append(q1);
		builder.append(" LEFT OUTER JOIN ");
		builder.append(q2);
		builder.append(" ON ");
		builder.append(t1.tmp_table).append(".").append(KEY).append(" = ").append(t2.tmp_table).append(".").append(KEY);
		builder.append(" )").append("ctb").append(++joinID);
		return builder.toString();
	}

	/**
	 * 俩表进行左外链接
	 * @param t1 当前驱动表的表名
	 * @param t2 右表
	 * @param q1 驱动表的子查询
	 * @param q2 右表的子查询
	 * @return
	 */
	public static String getJoinSql(String t1, HiveTable t2, String q1, String q2){
		StringBuilder builder = new StringBuilder();
		builder.append("( SELECT ");
		builder.append(t1).append(".*");
		//抽取出后置表的字段
		for(HiveField field: t2.fields){
			builder.append(" ,").append(field.new_fname);
		}
		builder.append(" FROM ");
		builder.append(q1);
		builder.append(" LEFT OUTER JOIN ");
		builder.append(q2);
		builder.append(" ON ");
		builder.append(t1).append(".").append(KEY).append(" = ").append(t2.tmp_table).append(".").append(KEY);
		builder.append(" )").append("ctb").append(++joinID);
		return builder.toString();
	}
	
	/**
	 * 获取完成的查询插入语句
	 * @param tb1
	 * @param tb2
	 * @param sql1
	 * @param sql2
	 * @return
	 */
	public static  String getCompleteSQL(String tb1, String tb2, String sql1, String sql2){
		StringBuilder builder = new StringBuilder();
		builder.append("EXPLAIN INSERT OVERWRITE TABLE ");
		builder.append(HIVE_TABLE);
		builder.append(" PARTITION(dt = '${date}') SELECT ");
		builder.append(tb1).append(".");
		builder.append(KEY);
		Collections.sort(HiveField.fields, compare);
		for(HiveField field: HiveField.fields){
			builder.append(" ,").append(field.new_fname);
		}
		builder.append(" FROM ");
		builder.append(sql1);
		builder.append(" LEFT OUTER JOIN ");
		builder.append(sql2);
		builder.append(" ON ");
		builder.append(tb1).append(".").append(KEY).append(" = ").append(tb2).append(".").append(KEY).append(";");
		return builder.toString();
	}
	
	public static String getHiveSelect(int[] tableId,Map<Integer, HiveTable> map ){
		List<String> SqlList = new ArrayList<String>();

		String sqlJoinString = new String();
		if (tableId.length < 3) {
			SqlList.add(map.get(tableId[0]).getSelect());
			SqlList.add(map.get(tableId[1]).getSelect());
			sqlJoinString = getCompleteSQL(
					"tb" + Integer.toString(tableId[0]),
					"tb" + Integer.toString(tableId[1]), SqlList.get(0),
					SqlList.get(1));
		} else {
			SqlList.add(map.get(tableId[0]).getSelect());
			SqlList.add(map.get(tableId[1]).getSelect());
			sqlJoinString = HiveJoin.getJoinSql(map.get(tableId[0]),
					map.get(tableId[1]), SqlList.get(0), SqlList.get(1));
			for (int i = 2; i < tableId.length - 1; i++) {
				SqlList.add(map.get(tableId[i]).getSelect());
				sqlJoinString = HiveJoin.getJoinSql(
						"ctb" + Integer.toString(HiveJoin.joinID),
						map.get(tableId[i]), sqlJoinString, SqlList.get(i));
			}
			SqlList.add(map.get(tableId[tableId.length - 1]).getSelect());
			sqlJoinString = HiveJoin.getCompleteSQL(
					"ctb" + Integer.toString(HiveJoin.joinID),
					"tb" + Integer.toString(tableId[tableId.length - 1]),
					sqlJoinString, SqlList.get(tableId.length - 1));
		}
		return sqlJoinString;
	}
	@Override
	public void getDate(ResultSet set) throws SQLException {
	}

}
