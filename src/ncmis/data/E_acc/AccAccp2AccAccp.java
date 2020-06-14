package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccAccp2AccAccp {
	
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

	//�������гжһ�Ʊ̨�ˣ�acc_accp 2 acc_accp
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("draft_no", "accp_no");//��Ʊ����
		map.put("appl_ccy", "'CNY'");//�������
		map.put("appl_amt", "accp_amount");//Ʊ����
		map.put("pre_sign_dt", "accp_issue_date");//Ʊ��ǩ������
		map.put("pre_due_dt", "accp_due_date");//Ʊ�ݵ�������
		map.put("cushion_flag", "decode(account_status,'6','Y','N')");//�Ƿ���
		map.put("cushion_amt", "''");//�����
		map.put("security_money_rt", "(security_money_amt+add_security_money_amt)/accp_amount");//��֤�����
		map.put("security_money_amt", "security_money_amt+add_security_money_amt");//��֤���Ԫ
		map.put("not_hegotiable_flg", "''");//����ת�ñ��
		map.put("issue_name", "cus_name");//��Ʊ������
		map.put("issue_bank_cde", "''");//��Ʊ����֯��������
		map.put("issue_bank_id", "issue_bank_id");//��Ʊ�˿����к�
		map.put("issue_bank_name", "issue_bank_name");//��Ʊ�˿�������
		map.put("issue_bank_acct", "issue_cus_acct");//��Ʊ�˿������˺�
		map.put("rec_name", "receiver_name");//�տ�������
		map.put("rec_bank_id", "receiver_bank_id");//�տ��˿������к�
		map.put("rec_bank_name", "receiver_bank");//�տ��˿���������
		map.put("rec_acct", "receiver_acc_id");//�տ��˿������˺�
		map.put("acpt_bank_typ", "decode(acpt_bank_type,'1','2','2','3','3','4','4','5','5','6')");//�ж�������
		map.put("acpt_bank_id", "acpt_bank_id");//�ж����к�
		map.put("acpt_bank_name", "acpt_bank_name");//�ж�������
		map.put("acpt_bank_cde", "''");//�ж�����֯��������
		map.put("acpt_bank_acct", "''");//�ж��˿������˺�
		map.put("draft_place", "''");//Ʊ��ǩ����
		map.put("lastest_upd_id", "cus_manager");//�����޸���
		map.put("lastest_upd_date", "latest_date");//�����޸�����
		map.put("acc_no", "dbms_random.string('l','30')");//̨�ʱ��
		map.put("pvp_loan_no", "''");//����������
		map.put("pvp_loan_no", "''");//����������
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("acc_sts", "decode(account_status,'0','1','1','2','6','3','9','4','2','5','7','4')");//̨��״̬
		map.put("cla", "cla");//�弶����
		//��ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("iqp_cde", "serno");//��������
		map1.put("serno", "serno");//ҵ����ˮ��
		map1.put("tnm_time_typ", "decode(biz_type,'212200','02','010000','01')");//Ʊ�����ͣ�01��Ʊ��02ֽƱ
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map1.put("dirt_name", "direction_name");//Ͷ������
		map1.put("remarks", "acpt_use");//�ж���;
		map1.put("prd_cde", "decode(special_type,'1','ACC0101','2','ACC0102')");//1һ��жң�2���Ҳ�
		map1.put("prd_name", "decode(special_type,'1','���гжһ�Ʊ','2','���Ҳ�����')");//1һ��жң�2���Ҳ�
		
		
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "accp_issue_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "accp_issue_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "accp_issue_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "accp_issue_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("instu_cde", "'0000'");//���˱���
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table acc_accp ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_accp ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_accp@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_accp a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_accp a set ("+keys1.toString().substring(1)+") =(SELECT "+values1.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101 b where b.cont_no= a.cont_no)";
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			//��prd_cdeΪ�յ�����Ĭ��ΪACC0101 Y88048201600014
			PreparedStatement up_prd_cde_ps=null;
			String prd_cde=" update acc_accp a set prd_cde = 'ACC0101' where prd_cde is null";
			up_prd_cde_ps=conn.prepareStatement(prd_cde);
			up_prd_cde_ps.execute();
			if(up_prd_cde_ps!=null){
				up_prd_cde_ps.close();
			}
			
			//�滻�س������з�
			PreparedStatement up_rec_bank_name_ps=null;
			String up_rec_bank_name=" UPDATE acc_accp a SET a.rec_bank_name = replace(rec_bank_name,CHR(10),'') ";
			up_rec_bank_name_ps=conn.prepareStatement(up_rec_bank_name);
			up_rec_bank_name_ps.execute();
			if(up_rec_bank_name_ps!=null){
				up_rec_bank_name_ps.close();
			}
			
			//�滻�س������з�
			PreparedStatement up_rec_bank_name_1_ps=null;
			String up_rec_bank_name_1=" UPDATE acc_accp a SET a.rec_bank_name = replace(rec_bank_name,CHR(13),'') ";
			up_rec_bank_name_1_ps=conn.prepareStatement(up_rec_bank_name_1);
			up_rec_bank_name_1_ps.execute();
			if(up_rec_bank_name_1_ps!=null){
				up_rec_bank_name_1_ps.close();
			}
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_accp_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_accp b  ) ";
			System.out.println(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps=conn.prepareStatement(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps.execute();
			if(delte_ctr_loan_cont_ps!=null){
				delte_ctr_loan_cont_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
}
