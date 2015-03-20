package yy.eagle.table.sj;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import yy.eagle.table.info.BasicID;
import yy.eagle.table.info.MProduct;
import yy.eagle.table.join.MysqlData;

public class InfoTable {
	public static List<String> field = new ArrayList<String>();
	public static final String SJ_DAY = "hiido_uf.tw_user_sj_info_day";
	public static final String SJ_ALL_DAY = "hiido_uf.tw_user_sj_info_all_day";
	public static String getSQL(){
		StringBuilder builder = new StringBuilder();
		builder.append("INSERT OVERWRITE TABLE ");
		builder.append(SJ_ALL_DAY);
		builder.append(" PARTITION(dt = '${date}') SELECT ");
		builder.append("IF(tb2.imei IS NULL, tb1.imei, tb2.imei) AS imei");
		builder.append(getIF("mac"));	
		for(String f: field)
			builder.append(getIF(f));
		builder.append(" FROM (SELECT * FROM ");
		builder.append(SJ_ALL_DAY).append(" WHERE dt = '${date-1}')tb1 ");
		builder.append("FULL OUTER JOIN (SELECT * FROM ");
		builder.append(SJ_DAY).append(" WHERE dt = '${date}')tb2 ");
		builder.append("ON tb1.imei = tb2.imei AND tb1.mac = tb2.mac;");
		return builder.toString();
	}
	
	public static String getIF(String field){
		return ",IF(tb2." + field + " IS NULL, tb1." + field + ", tb2." + field + ") AS " + field;
	}
	
	public static String combine(MProduct product){
		StringBuilder builder = new StringBuilder();
		builder.append(",IF(appkey = '");
		builder.append(product.product_key);
		builder.append("', ver,NULL)  AS ver_");
		builder.append(Integer.toString(product.id));

		builder.append(",IF(appkey = '");
		builder.append(product.product_key);
		builder.append("', `from`,NULL)  AS from_");
		builder.append(Integer.toString(product.id));
		field.add("ver_" + Integer.toString(product.id));
		field.add("from_" + Integer.toString(product.id));
		return builder.toString();	
	}
	
	public static void main(String[] args) {
		StringBuilder builder = new StringBuilder();
		List<MProduct> products = new ArrayList<MProduct>();
		products = MysqlData.getMProduct();
		builder.append("(SELECT imei, mac, ntm, sjp, sjm, sr");
		field.add("ntm");
		field.add("sjp");
		field.add("sjm");
		field.add("sr");
		field.add("cpunum");
		field.add("cpu");
		field.add("memory");
		Collections.sort(products, BasicID.compare);
		for(MProduct product: products)
			builder.append(combine(product));
		builder.append(" FROM default.yy_mbsdkinstall_original WHERE dt = '${date}')tb1");
		builder.append(" LEFT OUTER JOIN (SELECT imei, mac, most_appearance(cpunum) AS cpunum, most_appearance(cpu) AS cpu, most_appearance(memory) AS memory");
		builder.append(" FROM default.yy_mbsdkdevice_original WHERE dt <= '${date}' GROUP BY imei, mac)tb2 ");
		builder.append("ON tb1.imei = tb2.imei AND tb1.mac = tb2.mac GROUP BY tb1.imei, tb1.mac;");
		String tmpString = new String();
		tmpString = builder.toString();
		builder.delete(0, builder.length());
		builder.append("SELECT tb1.imei, tb1.mac");
		for(String string: field)
			builder.append(",most_appearance(").append(string).append(") AS ").append(string);
		builder.append(" FROM ").append(tmpString);
		//System.out.println(builder.toString());		
		System.out.println(getSQL());
	}
}
