package ncmis.data.D_cont.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

//押品估值信息,T_Guar_Eval_Info
public class GrtEval {
	


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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入估值信息
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','40')");//主键
		map.put("serno", "dbms_random.string('l','40')");//流水号
		map.put("guar_no", "guaranty_id");//押品编号
		map.put("EVAL_IN_OUT_TYPE", "''");//评估方式
		map.put("EVAL_TYPE", "'01'");//评估类型 01初评，02重估
		map.put("EVAL_IN_AMT", "eval_amt");//内部评估价值

		map.put("EVAL_IN_CURRENCY", "'CNY'");//内部评估价值币种
		map.put("EVAL_IN_DATE", "eval_date");//内部评估日期
		map.put("EVAL_OUT_AMT", "eval_amt");//外部评估价值
		
		map.put("EVAL_OUT_CURRENCY", "'CNY'");//外部评估价值币种
		map.put("EVAL_OUT_DATE", "eval_date");//外部评估日期
		map.put("EVAL_CURRENCY", "'CNY'");//我行确认价值币种
		map.put("EVAL_AMT", "max_mortagage_amt");//最终抵质押价值（元）
		map.put("EVAL_DATE", "eval_date");//确认时间
		map.put("EVAL_CUSID", "''");//评估人客户号
		map.put("EVAL_CUSNAME", "eval_org");//评估人客户名称
		
		map.put("APP_STATE", "'02'");//状态,01审批中（使用与押品重估时），02生效押品评估时；
		map.put("INPUT_ID", "create_user_no");//登记人
		map.put("INPUT_BR_ID", "input_br_id");//登记机构
		map.put("INPUT_DATE", "input_date");//登记日期
		map.put("EVAL_REMARKS", "''");//内部评估备注信息
		map.put("CAL_IN_WEIGHT", "''");//内部测算权重
		
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
			String de_cus_corp=" delete from T_Guar_Eval_Info where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Guar_Eval_Info ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_g_basic_info@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps1=null;
			String insert_cus_corp1=" INSERT INTO T_Guar_Eval_Info ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_p_basic_info@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp1);
			insert_cus_corp_ps1=conn.prepareStatement(insert_cus_corp1);
			insert_cus_corp_ps1.execute();
			if(insert_cus_corp_ps1!=null){
				insert_cus_corp_ps1.close();
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}
