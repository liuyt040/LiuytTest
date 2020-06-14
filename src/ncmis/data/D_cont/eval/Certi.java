package ncmis.data.D_cont.eval;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

//Ȩ֤��Ϣ
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

	//����Ȩ֤��Ϣ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		
		//��Ȩ֤ӳ������
		StringBuffer turn=new StringBuffer("");
		turn.append("'10001','01',");//����֤/���ز�֤  ��������Ȩ֤
		turn.append("'10002','43',");//����ʹ��֤  ��������Ȩ֤
		turn.append("'10003','11',");//�������Ǽ�֤��  ��������Ȩ֤
		turn.append("'10004','48',");//��������Ȩ�Ǽ�֤�� �������Ǽ�֤��
		turn.append("'10005','04',");//��Ȩ֤   ɭ����Դ�Ǽ�Ȩ֤
		turn.append("'10006','06',");//����ʹ��Ȩ֤   ɭ����Դ�Ǽ�Ȩ֤
//**	turn.append("'10007','06'");//��е�豸Ȩ֤   ɭ����Դ�Ǽ�Ȩ֤,ȱ��
		turn.append("'10008','30',");//��Ѻ����
		turn.append("'10009','31',");//�շ�Ȩ֤
		
		turn.append("'20007','35',");//����Ȩ֤
		turn.append("'20008','33',");//ר��Ȩ֤
		turn.append("'20009','31',");//�շ�Ȩ֤
		turn.append("'20010','05',");//�ɿ�Ȩ֤
		turn.append("'20001','13',");//�浥Ȩ֤
		turn.append("'20002','18',");//��ƱȨ֤
		turn.append("'20003','22',");//����Ȩ֤
		turn.append("'20004','17',");//ծȯȨ֤
		turn.append("'20005','39',");//�ֵ�/�ᵥȨ֤��Ŀǰû��
		turn.append("'20006','21',");//Ʊ��Ȩ֤
		turn.append("'20011','30'");//Ʊ��Ȩ֤
		
		
		
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','40')");//����
		map.put("guar_no", "guaranty_id");//ѺƷ���
//**		map.put("certi_name", "''");//Ȩ֤����
		map.put("certi_type_cd", "decode(right_cert_type_code,"+turn.toString()+")");//Ȩ֤����
		map.put("certi_record_id", "right_cert_no");//Ȩ��ƾ֤��
		map.put("certi_org_name", "right_org");//Ȩ���Ǽǻ���
		map.put("certi_start_date", "''");//Ȩ֤��֤����
		map.put("certi_end_date", "''");//Ȩ֤��������
		map.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 �����
		map.put("INPUT_ID", "create_user_no");//�Ǽ���
		map.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
		map.put("bch_cde", "input_br_id");//���ܻ���
		map.put("INPUT_DATE", "input_date");//�Ǽ�����
		map.put("destroy_ind", "'N'");//�ڲ�������ע��Ϣ
		map.put("in_date", "in_date");//���ʱ��
		map.put("out_date", "out_date");//����ʱ��
		
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
			 * ��Ѻȱ��Ȩ֤����
			 */
			Map<String,String> map1=new HashMap<String,String>();
			map1=new HashMap<String, String>();
			map1.put("pk_id", "dbms_random.string('l','40')");//����
			map1.put("guar_no", "guaranty_id");//ѺƷ���
	//**		map.put("certi_name", "''");//Ȩ֤����
			map1.put("certi_type_cd", "decode(right_cert_type_code,"+turn.toString()+")");//Ȩ֤����
			map1.put("certi_record_id", "right_cert_no");//Ȩ��ƾ֤��
			map1.put("certi_start_date", "''");//Ȩ֤��֤����
			map1.put("certi_end_date", "''");//Ȩ֤��������
			map1.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 �����
			map1.put("INPUT_ID", "create_user_no");//�Ǽ���
			map1.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
			map1.put("INPUT_DATE", "input_date");//�Ǽ�����
			map1.put("destroy_ind", "'N'");//�ڲ�������ע��Ϣ
			map1.put("in_date", "in_date");//���ʱ��
			map1.put("out_date", "out_date");//����ʱ��
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
			 * ����Ȩ֤
			 */
			Map<String,String> map2=new HashMap<String,String>();
			map2=new HashMap<String, String>();
			map2.put("pk_id", "dbms_random.string('l','40')");//����
			map2.put("guar_no", "guaranty_id");//ѺƷ���
	//**		map.put("certi_name", "''");//Ȩ֤����
			map2.put("certi_type_cd", "decode(gage_type,'10003','02','10001','03','10031','03','10006','12','10008','47','44')");//Ȩ֤����
			map2.put("certi_record_id", "book_no");//Ȩ��ƾ֤��
			map2.put("certi_start_date", "book_date");//Ȩ֤��֤����
			map2.put("certi_end_date", "''");//Ȩ֤��������
			map2.put("certi_state", "decode(status_code,'10006','02','10007','02','10009','02','10008','04','01')");//02 �����
			map2.put("INPUT_ID", "create_user_no");//�Ǽ���
			map2.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
			map2.put("INPUT_DATE", "input_date");//�Ǽ�����
			map2.put("destroy_ind", "'N'");//�ڲ�������ע��Ϣ
			map2.put("certi_org_name", "book_org");//����Ȩ֤��Ѻ�Ǽǲ���
			map2.put("in_date", "in_date");//����Ȩ֤���ʱ��
			map2.put("out_date", "out_date");//����Ȩ֤����ʱ��
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
	
	//����Ȩ֤�ĵ�����ͬ���
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
