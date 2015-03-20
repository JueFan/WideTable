package yy.eagle.table.join;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import yy.eagle.table.info.BasicID;
import yy.eagle.table.info.SolrField;

public class SolrBuild extends BasicID {

	/**
	 * 生成入库SOLR表的HQL
	 * @param fields	solr表需要的字段信息
	 * @param tableId	引用表的ID（避免引用出错）
	 * @return
	 */
	public static String getSolrSelect(List<SolrField> fields, int[] tableId) {
		Set<Integer> set = new HashSet<Integer>();
		for (int i : tableId)
			set.add(i);
		StringBuilder builder = new StringBuilder();
		builder.append("EXPLAIN INSERT OVERWRITE TABLE ").append(SOLR_TABLE)
				.append(" PARTITION(dt = '${date}')  SELECT ").append(KEY);
		
		Collections.sort(fields, compare);	//按照Mysql中的自增ID排序
		for (SolrField field : fields) {
			if (set.contains(field.table_id))
			builder.append(",").append(field.fname);	//读取需要用到的字段名
		}
		builder.append(" FROM (");
		builder.append("SELECT ");
		builder.append(KEY);
		for (SolrField field : fields) {
			if (set.contains(field.table_id))
				builder.append(field.combine());	//读取表生成的字段逻辑
		}
		builder.append(" , row_number () over (distribute BY ").append(KEY)
				.append(" SORT BY ").append(KEY).append(" ) AS row_cnt FROM ");
		builder.append(HIVE_TABLE);
		builder.append(" WHERE dt = '${date}')tb1 WHERE row_cnt = 1;");
		return builder.toString();
	}

	@Override
	public void getDate(ResultSet set) throws SQLException {
	}
}
