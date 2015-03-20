package yy.eagle.table.join;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import yy.eagle.table.info.HiveField;
import yy.eagle.table.info.HiveTable;
import yy.eagle.table.info.MProduct;
import yy.eagle.table.info.SolrField;
import yy.eagle.table.info.UdfFunction;

public class MysqlData {
	/**
	 * 读取小宽表及对应的字段信息
	 * 
	 * @return 以小宽表ID为key的映射
	 */
	public static Map<Integer, HiveTable> getTableMap() {
		Map<Integer, HiveTable> tableMap = new HashMap<Integer, HiveTable>();
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/cube", "root", "baivfhpiaqg");
			Statement stmt = connect.createStatement();

			// Hive表加载
			ResultSet rs = stmt
					.executeQuery("select * from widetable_hive_table");
			while (rs.next()) {
				HiveTable table = new HiveTable();
				table.getDate(rs);
				tableMap.put(table.id, table);
			}
			rs.close();

			// Hive表字段信息加载
			rs = stmt
					.executeQuery("select * from widetable_hive_field where `status` <> 'invalid'");
			while (rs.next()) {
				HiveField field = new HiveField();
				field.getDate(rs);
				tableMap.get(field.table_id).fields.add(field);
			}
			rs.close();

		} catch (Exception exception) {
			System.err.println("读取表信息失败");
		}
		return tableMap;
	}

	/**读取Solr字段*/
	public static Map<String, List<SolrField>> getSolrField() {
		Map<String, List<SolrField>> map = new HashMap<String, List<SolrField>>();
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {

			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/cube", "root", "baivfhpiaqg");
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt
					.executeQuery("select * from widetable_solr_field");

			while (rs.next()) {	
				SolrField field = new SolrField();
				field.getDate(rs);
				if (map.containsKey(field.act)) {
					map.get(field.act).add(field);
				} else {
					List<SolrField> list = new ArrayList<SolrField>();
					list.add(field);
					map.put(field.act, list);
				}
			}
		} catch (Exception exception) {
			//System.err.println("读取Solr字段失败");
		}
		return map;
	}

	/**
	 * 读取自定义函数信息
	 * @author JueFan
	 * @return 
	 */
	public static Map<String, UdfFunction> getUdfMap() {
		Map<String, UdfFunction> udfMap = new HashMap<String, UdfFunction>();
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/cube", "root", "baivfhpiaqg");
			Statement stmt = connect.createStatement();

			// Hive自定义函数加载
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM widetable_udf_info");
			while (rs.next()) {
				UdfFunction udfFunction = new UdfFunction(rs);
				udfMap.put(rs.getString("name"), udfFunction);
			}
			rs.close();

		} catch (Exception exception) {
			System.err.println("读取自定义函数失败");
		}
		return udfMap;
	}
	
	
	/**
	 * 读取移动产品信息
	 * @author JueFan
	 * @return 
	 */
	public static List<MProduct> getMProduct() {
		List<MProduct> list = new ArrayList<MProduct>();
		try {
			Class.forName("com.mysql.jdbc.Driver"); // 加载MYSQL JDBC驱动程序
		} catch (Exception e) {
			System.out.print("Error loading Mysql Driver!");
			e.printStackTrace();
		}
		try {
			Connection connect = DriverManager.getConnection(
					"jdbc:mysql://localhost:3306/cube", "root", "baivfhpiaqg");
			Statement stmt = connect.createStatement();

			// Hive自定义函数加载
			ResultSet rs = stmt
					.executeQuery("SELECT * FROM m_product");
			while (rs.next()) {
				MProduct product = new MProduct();
				product.getDate(rs);
				list.add(product);
			}
			rs.close();

		} catch (Exception exception) {
			System.err.println("读取移动产品信息失败");
		}
		return list;
	}
}
