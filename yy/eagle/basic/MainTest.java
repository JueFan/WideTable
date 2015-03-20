package yy.eagle.basic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import yy.eagle.table.info.HiveTable;
import yy.eagle.table.join.HiveJoin;
import yy.eagle.table.join.MysqlData;
import yy.eagle.table.join.UserTable;

public class MainTest {
	public static void main(String[] args) {
		Map<Integer, HiveTable> map = new HashMap<Integer, HiveTable>();
		map = MysqlData.getTableMap();
		UserTable userTable = new UserTable();
		
		// 操作表的ID
		int[] tableId = { 1, 3, 5, 6, 7, 8, 12, 13, 14, 15, 16 };
		List<String> SqlList = new ArrayList<String>();

		String sqlJoinString = new String();
		if (tableId.length < 3) {
			SqlList.add(map.get(tableId[0]).getSelect());
			SqlList.add(map.get(tableId[1]).getSelect());
			sqlJoinString = HiveJoin.getCompleteSQL(
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

		System.out.println(sqlJoinString);
	}

}
