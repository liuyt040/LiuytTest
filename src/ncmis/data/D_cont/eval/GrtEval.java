package ncmis.data.D_cont.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

//ѺƷ��ֵ��Ϣ,T_Guar_Eval_Info
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

	//�����ֵ��Ϣ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','40')");//����
		map.put("serno", "dbms_random.string('l','40')");//��ˮ��
		map.put("guar_no", "guaranty_id");//ѺƷ���
		map.put("EVAL_IN_OUT_TYPE", "''");//������ʽ
		map.put("EVAL_TYPE", "'01'");//�������� 01������02�ع�
		map.put("EVAL_IN_AMT", "eval_amt");//�ڲ�������ֵ

		map.put("EVAL_IN_CURRENCY", "'CNY'");//�ڲ�������ֵ����
		map.put("EVAL_IN_DATE", "eval_date");//�ڲ���������
		map.put("EVAL_OUT_AMT", "eval_amt");//�ⲿ������ֵ
		
		map.put("EVAL_OUT_CURRENCY", "'CNY'");//�ⲿ������ֵ����
		map.put("EVAL_OUT_DATE", "eval_date");//�ⲿ��������
		map.put("EVAL_CURRENCY", "'CNY'");//����ȷ�ϼ�ֵ����
		map.put("EVAL_AMT", "max_mortagage_amt");//���յ���Ѻ��ֵ��Ԫ��
		map.put("EVAL_DATE", "eval_date");//ȷ��ʱ��
		map.put("EVAL_CUSID", "''");//�����˿ͻ���
		map.put("EVAL_CUSNAME", "eval_org");//�����˿ͻ�����
		
		map.put("APP_STATE", "'02'");//״̬,01�����У�ʹ����ѺƷ�ع�ʱ����02��ЧѺƷ����ʱ��
		map.put("INPUT_ID", "create_user_no");//�Ǽ���
		map.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
		map.put("INPUT_DATE", "input_date");//�Ǽ�����
		map.put("EVAL_REMARKS", "''");//�ڲ�������ע��Ϣ
		map.put("CAL_IN_WEIGHT", "''");//�ڲ�����Ȩ��
		
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
