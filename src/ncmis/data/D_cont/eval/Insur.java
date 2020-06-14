package ncmis.data.D_cont.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

//������Ϣ
public class Insur {

	


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

	//���뱣����Ϣ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','40')");//����
		map.put("guar_no", "guaranty_id");//ѺƷ���
		map.put("insur_policy_num", "assurance_no");//ѺƷ��������
		map.put("insur_amt", "assurance_amt");//���ս��
		map.put("start_date", "assurance_date");//��ʼ����
		map.put("end_date", "end_date");//��������
		map.put("insur_org_name", "co_name");//���չ�˾����
		
		map.put("INPUT_ID", "create_user_no");//�Ǽ���
		map.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
		map.put("INPUT_DATE", "input_date");//�Ǽ�����
		map.put("insur_state", "'A'");//�ڲ�������ע��Ϣ
		
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
			String de_cus_corp=" delete from T_Guar_Insur_Info where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Guar_Insur_Info ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_g_basic_info@CMISTEST2101 b where b.assurance_flg='1' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement insert_cus_corp_ps1=null;
			String insert_cus_corp1=" INSERT INTO T_Guar_Insur_Info ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_p_basic_info@CMISTEST2101 b where b.assurance_flg='1'  )";
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
