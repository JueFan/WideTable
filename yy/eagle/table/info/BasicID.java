package yy.eagle.table.info;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public abstract class BasicID {

	public int id = 0; //Mysql中的唯一ID
	
	public static String KEY ;	//宽表主键
	public static String STRUCT;		//宽表主键类型
	
	public static String HIVE_TABLE;
	public static String SOLR_TABLE;
	
	public static ComparatorID compare = new ComparatorID();
	
	public static final String CONFIG = "SET hive.exec.parallel=TRUE;\n" +
			"SET mapred.output.compress=TRUE;\n" +
			"SET hive.exec.compress.output=TRUE;\n" +
			"SET mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;\n" +
			"SET io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;\n" +
			"SET mapred.output.compression.type=BLOCK;\n" +
			"SET hive.auto.convert.join=FALSE;\n";

	
	/**获取Mysql中的数据
	 * @throws SQLException */
	public abstract void getDate(ResultSet set) throws SQLException;
}

class ComparatorID implements Comparator<BasicID>{
	public int compare(BasicID arg0, BasicID arg1) {
		int flag = arg0.id > arg1.id ? 1 : -1;
		return flag;
	}
}