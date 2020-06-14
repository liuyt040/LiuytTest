package ncmis.data.B_cus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * ��ֲͬҵ�ͻ���Ϣ��s_bank_serno 2 cus_same_org
 * 
 */
public class Bank2CusSameOrg {

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
			test3(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//����ͬҵ�ͻ�������Ϣ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("cus_id", "bank_srrno");//�ͻ���
		map.put("large_bank_no", "bank_srrno");//�к�
		map.put("cus_name", "bank_name");//����
		map.put("same_org_typ", "'01'");//���ڽ���ͬҵ�ͻ�
		map.put("main_usr", "'1594'");//���ܿͻ�����
		map.put("main_bch", "'88888'");//���ܻ���
		map.put("cus_sts", "'20'");//�ͻ�״̬����ʽ�ͻ�
		map.put("instu_cde", "'0000'");//���˱���
		
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
			String de_cus_corp=" TRUNCATE table cus_same_org ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_same_org ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM s_bank_serno@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	//���뼯�ſͻ�������Ϣ Cus_Grp_Info 2  Cus_Grp_App
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("SERNO", "grp_no");//���ſͻ�������ˮ��
		map.put("grp_id", "grp_no");//����(����)��ţ�
		map.put("grp_name", "grp_name");//����(����)����
		map.put("parent_cus_id", "parent_cus_id");//��(����)�ͻ���
		map.put("parent_cus_name", "parent_cus_name");//��(����)�ͻ�����
		map.put("cert_typ", "20100");//���������(����)֤������
		map.put("cert_code", "parent_org_code");//���������(����)֤������
		map.put("grp_detail", "grp_detail");//���������ţ����˵��
		map.put("input_id", "input_user_id");//�Ǽ���
		map.put("input_br_id", "input_br_id");//�Ǽǻ���
		map.put("cus_manager", "input_user_id");//���ܿͻ�����
		map.put("main_br_id", "input_br_id");//�Ǽǻ���
		map.put("input_date", "input_date");//�Ǽ�����
		map.put("available_ind", "'A'");//�Ƿ���Ч
		map.put("wf_appr_sts", "'997'");//����״̬
		
		
		map.put("instu_cde", "'0000'");//
		
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
			String de_cus_corp=" TRUNCATE table Cus_Grp_App ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Cus_Grp_App ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM Cus_Grp_Info@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���뼯�ſͻ��ӹ�˾��Ϣ Cus_Grp_Member 2  Cus_Grp_Member_App
	public static void test3(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("SERNO", "grp_no");//���ſͻ�������ˮ��
		map.put("grp_id", "grp_no");//����(����)��ţ�
		map.put("cus_id", "cus_id");//��Ա�ͻ���
		map.put("cus_name", "cus_name");//��Ա�ͻ�����
		map.put("grp_corre_type", "decode(grp_corre_type,'1','1','2','2','9','3')");//�������Ź�����ϵ����
		map.put("grp_corre_detail", "grp_corre_detail");//���������ţ����˵��
		map.put("input_id", "input_user_id");//�Ǽ���
		map.put("input_br_id", "input_br_id");//�Ǽǻ���
		map.put("cus_manager", "input_user_id");//���ܿͻ�����
		map.put("main_br_id", "input_br_id");//�Ǽǻ���
		map.put("input_date", "input_date");//�Ǽ�����
		map.put("con_state", "'2'");//ȷ��״̬
		
		map.put("instu_cde", "'0000'");//
		
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
			String de_cus_corp=" TRUNCATE table Cus_Grp_Member_App ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Cus_Grp_Member_App ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM Cus_Grp_Member@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_cus_detail_ps=null;
			String up_cus_detail=" UPDATE Cus_Grp_Member_App a SET a.serno= ( SELECT b.serno FROM Cus_Grp_App b WHERE b.grp_id=a.grp_id  ) WHERE a.grp_id IN (  SELECT c.grp_id FROM Cus_Grp_App c  ) ";
			System.out.println(up_cus_detail);
			up_cus_detail_ps=conn.prepareStatement(up_cus_detail);
			up_cus_detail_ps.execute();
			if(up_cus_detail_ps!=null){
				up_cus_detail_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
}
