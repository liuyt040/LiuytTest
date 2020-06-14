package ncmis.data.D_cont.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

//权证信息
public class Certi {

	


//	private static List<String> list=null;
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
//		conn = DriverManager.getConnection(
//				"jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入权证信息
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		
		//产权证映射类型
		StringBuffer turn=new StringBuffer("");
		turn.append("'10001','01',");//房产证/房地产证  房屋所有权证
		turn.append("'10002','43',");//土地使用证  房屋所有权证
		turn.append("'10003','11',");//机动车登记证书  房屋所有权证
		turn.append("'10004','48',");//船舶所有权登记证书 机动车登记证书
		turn.append("'10005','04',");//林权证   森林资源登记权证
		turn.append("'10006','06',");//海域使用权证   森林资源登记权证
//**	turn.append("'10007','06'");//机械设备权证   森林资源登记权证,缺少
		turn.append("'10008','30',");//抵押其他
		turn.append("'10009','31',");//收费权证
		
		turn.append("'20007','35',");//著作权证
		turn.append("'20008','33',");//专利权证
		turn.append("'20009','31',");//收费权证
		turn.append("'20010','05',");//采矿权证
		turn.append("'20001','13',");//存单权证
		turn.append("'20002','18',");//汇票权证
		turn.append("'20003','22',");//保单权证
		turn.append("'20004','17',");//债券权证
		turn.append("'20005','39',");//仓单/提单权证，目前没有
		turn.append("'20006','21',");//票据权证
		turn.append("'20011','30'");//票据权证
		
		
		
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','40')");//主键
		map.put("guar_no", "guaranty_id");//押品编号
//**		map.put("certi_name", "''");//权证名称
		map.put("certi_type_cd", "decode(right_cert_type_code,"+turn.toString()+")");//权证类型
		map.put("certi_record_id", "right_cert_no");//权利凭证号
		map.put("certi_org_name", "right_org");//权属登记机关
		map.put("certi_start_date", "''");//权证发证日期
		map.put("certi_end_date", "''");//权证到期日期
		map.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 已入库
		map.put("INPUT_ID", "create_user_no");//登记人
		map.put("INPUT_BR_ID", "input_br_id");//登记机构
		map.put("bch_cde", "input_br_id");//保管机构
		map.put("INPUT_DATE", "input_date");//登记日期
		map.put("destroy_ind", "'N'");//内部评估备注信息
		map.put("in_date", "in_date");//入库时间
		map.put("out_date", "out_date");//出库时间
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from T_Guar_Certi_Rela a  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Guar_Certi_Rela ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_g_basic_info@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			/*
			 * 质押缺少权证机构
			 */
			Map<String,String> map1=new HashMap<String,String>();
			map1=new HashMap<String, String>();
			map1.put("pk_id", "dbms_random.string('l','40')");//主键
			map1.put("guar_no", "guaranty_id");//押品编号
	//**		map.put("certi_name", "''");//权证名称
			map1.put("certi_type_cd", "decode(right_cert_type_code,"+turn.toString()+")");//权证类型
			map1.put("certi_record_id", "right_cert_no");//权利凭证号
			map1.put("certi_start_date", "''");//权证发证日期
			map1.put("certi_end_date", "''");//权证到期日期
			map1.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 已入库
			map1.put("INPUT_ID", "create_user_no");//登记人
			map1.put("INPUT_BR_ID", "input_br_id");//登记机构
			map1.put("INPUT_DATE", "input_date");//登记日期
			map1.put("destroy_ind", "'N'");//内部评估备注信息
			map1.put("in_date", "in_date");//入库时间
			map1.put("out_date", "out_date");//出库时间
			StringBuffer keys1=new StringBuffer("");
			StringBuffer values1=new StringBuffer("");
			for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
				keys1.append(","+entry1.getKey());
				values1.append(","+entry1.getValue());
			}
			PreparedStatement insert_cus_corp_ps1=null;
			String insert_cus_corp1=" INSERT INTO T_Guar_Certi_Rela ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM grt_p_basic_info@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp1);
			insert_cus_corp_ps1=conn.prepareStatement(insert_cus_corp1);
			insert_cus_corp_ps1.execute();
			if(insert_cus_corp_ps1!=null){
				insert_cus_corp_ps1.close();
			}

			/*
			 * 他项权证
			 */
			Map<String,String> map2=new HashMap<String,String>();
			map2=new HashMap<String, String>();
			map2.put("pk_id", "dbms_random.string('l','40')");//主键
			map2.put("guar_no", "guaranty_id");//押品编号
	//**		map.put("certi_name", "''");//权证名称
			map2.put("certi_type_cd", "decode(gage_type,'10003','02','10001','03','10031','03','10006','12','10008','47','44')");//权证类型
			map2.put("certi_record_id", "book_no");//权利凭证号
			map2.put("certi_start_date", "book_date");//权证发证日期
			map2.put("certi_end_date", "''");//权证到期日期
			map2.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 已入库
			map2.put("INPUT_ID", "create_user_no");//登记人
			map2.put("INPUT_BR_ID", "input_br_id");//登记机构
			map2.put("INPUT_DATE", "input_date");//登记日期
			map2.put("destroy_ind", "'N'");//内部评估备注信息
			map2.put("certi_org_name", "book_org");//他项权证抵押登记部门
			map2.put("in_date", "in_date");//他项权证入库时间
			map2.put("out_date", "out_date");//他项权证出库时间
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			PreparedStatement insert_cus_corp_ps2=null;
			String insert_cus_corp2=" INSERT INTO T_Guar_Certi_Rela ("+keys2.toString().substring(1)+") (SELECT "+values2.toString().substring(1)+" FROM grt_g_basic_info@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp2);
			insert_cus_corp_ps2=conn.prepareStatement(insert_cus_corp2);
			insert_cus_corp_ps2.execute();
			if(insert_cus_corp_ps2!=null){
				insert_cus_corp_ps2.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//更新权证的担保合同编号
	public static void test2(Connection conn) throws SQLException {
		PreparedStatement insert_cus_corp_ps2=null;
		String insert_cus_corp2=" UPDATE T_Guar_Certi_Rela a SET a.line_grt_cont_no = ( SELECT B.guar_cont_no FROM grt_guaranty_re@CMISTEST2101 b  WHERE b.GUARanty_id=a.guar_no ) WHERE a.line_grt_cont_no IS NULL ";
		System.out.println(insert_cus_corp2);
		insert_cus_corp_ps2=conn.prepareStatement(insert_cus_corp2);
		insert_cus_corp_ps2.execute();
		if(insert_cus_corp_ps2!=null){
			insert_cus_corp_ps2.close();
		}
	}
	

}
