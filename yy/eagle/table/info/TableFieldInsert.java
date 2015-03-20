package yy.eagle.table.info;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import yy.eagle.basic.FileIO;

/**
 * 表字段原始信息入库Mysql
 * 通过Hcat获取表的结构(desc)
 * 复制具体结果进TableFieldInsert.txt文件
 * 事先将表名填写进cube.widetable_info中，更改下面的TableId值为库中对应的id值即可
 * @author JueFan
 */
public class TableFieldInsert {
	public static int TableId = 20;
	public static void main(String[] args) {
		FileIO fileIO = new FileIO();
		fileIO.SetfileName("./file/TableFieldInsert.txt");
		fileIO.FileRead();
		List<String> list = new ArrayList<String>();
		list = fileIO.cloneList();
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

			for(String string: list){
				String insertString = "INSERT INTO widetable_hive_field(table_id,status,fname,struct) VALUE('" + TableId + "','new','" + string.split(" ")[0] + "','" +  string.split(" ")[string.split(" ").length - 1] + "');";
				stmt.execute(insertString);
			}

		} catch (Exception exception) {
			System.err.println("Mysql链接未成功......");
		}
	}

}

