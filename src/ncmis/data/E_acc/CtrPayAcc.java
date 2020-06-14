package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * ��ͬ��ص��˺���Ϣ
 * �޷�ȡ���˺����
 */
public class CtrPayAcc {

//	private static List<String> list=null;
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			test3(conn);
			test4(conn);
			test5(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//������ͨ���ί�д������ó�����ʺ�ͬ�˺�
	public static void test1(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map.put("cont_no", "loan_no");//��ͬ��
		map.put("acct_seq", "acc_sn");//�˻����
		map.put("pay_acct_typ", "decode(acct_typ,'PAYM','01','ACTV','02','TURE_P','04','TURE_I','05')");//�˺�����
		map.put("acct_cus_id", "''");//�˻��˿ͻ���
		map.put("acct_cus_name", "acct_name");//�˻��˿ͻ�����
		map.put("payback_acct", "acct_no");//�˺�
		map.put("acct_rpym_ac_ccy", "acct_ccy_cde");//����
		map.put("pay_bank_no", "''");//�˻�������
		map.put("pay_bank_name", "''");//�˻�������
		map.put("pay_org_no", "''");//�˻�������
		map.put("pay_org_name", "''");//��������
		map.put("acc_name", "acct_name");//�˻��˿ͻ�����
		
		
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
			String de_cus_corp=" delete from ctr_pay_acc   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lm_acct_info@YCLOANS b   where loan_no IN ( "
									+" SELECT a.bill_no FROM acc_loan a " 
									+" UNION ALL "
									+" SELECT b.bill_no FROM acc_corp_loan b " 
									+" UNION ALL "
									+" SELECT c.bill_no FROM acc_fact c "
									+" UNION ALL "
									+" SELECT d.bill_no FROM acc_tf_app d ) )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_ctr_pay_ps=null;
			String up_ctr_pay=" UPDATE ctr_pay_acc a SET a.cont_no = ( SELECT distinct cont_no FROM ( "
				+" SELECT a.bill_no,a.cont_no FROM acc_loan a "
				+" UNION ALL "
				+" SELECT b.bill_no,b.cont_no FROM acc_corp_loan b " 
				+" UNION ALL "
				+" SELECT c.bill_no,c.cont_no FROM acc_fact c "
				+" UNION ALL "
				+" SELECT d.bill_no,d.cont_no FROM acc_tf_app d " 
				+") g WHERE g.bill_no=a.cont_no  ) ";
			System.out.println(up_ctr_pay);
			up_ctr_pay_ps=conn.prepareStatement(up_ctr_pay);
			up_ctr_pay_ps.execute();
			if(up_ctr_pay_ps!=null){
				up_ctr_pay_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
	//�������к�ͬ�˺�
	public static void test2(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map.put("cont_no", "cont_no");//��ͬ��
		map.put("payback_acct", "issue_cus_acct");//�˺�
		map.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map.put("pay_acct_typ", "'06'");//�˺�����,�����˺�
		map.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map.put("acct_cus_name", "cus_name");//�˻��˿ͻ�����
		map.put("acct_rpym_ac_ccy", "'CNY'");//����
		map.put("pay_bank_no", "''");//�˻������ţ������ţ�
		
		map.put("pay_bank_name", "''");//�˻�������
		map.put("pay_org_no", "''");//�˻�������
		map.put("pay_org_name", "''");//��������
		map.put("acc_name", "cus_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		Map<String,String> map1=new HashMap<String, String>();//��֤���˺�
		map1.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map1.put("cont_no", "cont_no");//��ͬ��
		map1.put("payback_acct", "security_money_ac_no");//�˺�
		map1.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map1.put("pay_acct_typ", "'03'");//�˺�����,��֤���˺�
		map1.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map1.put("acct_cus_name", "cus_name");//�˻��˿ͻ�����
		map1.put("acct_rpym_ac_ccy", "'CNY'");//����
		map1.put("pay_bank_no", "''");//�˻������ţ������ţ�
		
		map1.put("pay_bank_name", "''");//�˻�������
		map1.put("pay_org_no", "''");//�˻�������
		map1.put("pay_org_name", "''");//��������
		map1.put("acc_name", "cus_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement in_acc_accp_ps=null;
			String in_acc_accp=" INSERT INTO ctr_pay_acc ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101) ";
			System.out.println(in_acc_accp);
			in_acc_accp_ps=conn.prepareStatement(in_acc_accp);
			in_acc_accp_ps.execute();
			if(in_acc_accp_ps!=null){
				in_acc_accp_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���뱣����ͬ�˺�,���º���Ҫ�������˺���Ϣ
	public static void test3(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map.put("cont_no", "cont_no");//��ͬ��
		map.put("payback_acct", "cus_account");//�˺�
		map.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map.put("pay_acct_typ", "'06'");//�˺�����,�����˺�
		map.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map.put("acct_cus_name", "cus_account_name");//�˻��˿ͻ�����
		map.put("acct_rpym_ac_ccy", "'CNY'");//����
		map.put("pay_bank_no", "''");//�˻������ţ������ţ�
		map.put("pay_bank_name", "''");//�˻�������
		map.put("pay_org_no", "''");//�˻�������
		map.put("pay_org_name", "''");//��������
		map.put("acc_name", "cus_account_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		Map<String,String> map1=new HashMap<String, String>();//��֤���˺�
		map1.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map1.put("cont_no", "cont_no");//��ͬ��
		map1.put("payback_acct", "security_money_ac_no");//�˺�
		map1.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map1.put("pay_acct_typ", "'03'");//�˺�����,��֤���˺�
		map1.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map1.put("acct_cus_name", "cus_name");//�˻��˿ͻ�����
		map1.put("acct_rpym_ac_ccy", "'CNY'");//����
		map1.put("pay_bank_no", "''");//�˻������ţ������ţ�
		
		map1.put("pay_bank_name", "''");//�˻�������
		map1.put("pay_org_no", "''");//�˻�������
		map1.put("pay_org_name", "''");//��������
		map1.put("acc_name", "cus_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM pvp_cvrg_app@CMISTEST2101  WHERE approve_status ='997' AND chargeoff_status='3' ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement in_acc_accp_ps=null;
			String in_acc_accp=" INSERT INTO ctr_pay_acc ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101) ";
			System.out.println(in_acc_accp);
			in_acc_accp_ps=conn.prepareStatement(in_acc_accp);
			in_acc_accp_ps.execute();
			if(in_acc_accp_ps!=null){
				in_acc_accp_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
	//�������ֺ�ͬ�˺�,���ֲ��漰��֤��
	public static void test4(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map.put("cont_no", "cont_no");//��ͬ��
		map.put("payback_acct", "cus_acct");//�˺�
		map.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map.put("pay_acct_typ", "'06'");//�˺�����,�����˺�
		map.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map.put("acct_cus_name", "cus_name");//�˻��˿ͻ�����
		map.put("acct_rpym_ac_ccy", "'CNY'");//����
		map.put("pay_bank_no", "''");//�˻������ţ������ţ�
		
		map.put("pay_bank_name", "''");//�˻�������
		map.put("pay_org_no", "''");//�˻�������
		map.put("pay_org_name", "''");//��������
		map.put("acc_name", "cus_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//��������֤��֤���˺ţ���ϵͳû�н����˺ţ������˺��ڹ�������
	public static void test5(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32λ��ˮ��
		map.put("cont_no", "cont_no");//��ͬ��
		map.put("payback_acct", "security_money_ac_no");//�˺�
		map.put("acct_seq", "''");//�˻����,Ĭ�ϻ���1
		map.put("pay_acct_typ", "'03'");//�˺�����,�����˺�
		map.put("acct_cus_id", "cus_id");//�˻��˿ͻ���
		map.put("acct_cus_name", "cus_name");//�˻��˿ͻ�����
		map.put("acct_rpym_ac_ccy", "security_cur_type");//����
		map.put("pay_bank_no", "''");//�˻������ţ������ţ�
		
		map.put("pay_bank_name", "''");//�˻�������
		map.put("pay_org_no", "''");//�˻�������
		map.put("pay_org_name", "''");//��������
		map.put("acc_name", "cus_name");//�˻��˿ͻ�����
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM pvp_itsmstout_app@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

}

