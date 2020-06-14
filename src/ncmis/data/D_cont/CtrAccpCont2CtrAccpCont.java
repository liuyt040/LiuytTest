package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * ����Э�� ctr_accp_cont 2 ctr_accp_cont
 */
public class CtrAccpCont2CtrAccpCont {

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
			if(first(conn)){
				test1(conn);
			}else{
				System.out.println("��ϵͳ���ں�ͬ��ǩ������δ�ſ�ĺ�ͬ����Ҫȫ����ע������������ֲ�������������ų���");
			}
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	public static boolean first(Connection conn){
		boolean cont_num=true;
		PreparedStatement count_ps=null;
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_accp_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_accp@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
		try {
			count_ps=conn.prepareStatement(count);
			ResultSet rs =count_ps.executeQuery();
			while(rs.next()){
				cont_num=false;
				System.out.println("��ͬ�ţ�"+rs.getString("CONT_NO")+" ����̨�ˣ�"+rs.getString("LIMIT_ACC_NO")+" ����Э�飺"+rs.getString("LIMIT_MA_NO"));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		return cont_num;
	}
	
	//��������Э��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("sign_dt", "sign_date");//Э��ǩ������
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("cont_end_dt", "pre_due_date");//��ͬ������
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("cl_typ", "'ACCP'");//�Ŵ�Ʒ��
		map.put("prd_cde", "decode(special_type,'1','ACC0101','2','ACC0102')");//1һ��жң�2���Ҳ�
		map.put("prd_name", "decode(special_type,'1','���гжһ�Ʊ','2','���Ҳ�����')");//1һ��жң�2���Ҳ�
		map.put("tnm_time_typ", "decode(biz_type,'212200','02','010000','01')");//Ʊ�����ͣ�01��Ʊ��02ֽƱ
		map.put("cont_typ", "'03'");//��ͬ����
		map.put("acpt_typ", "decode(acpt_type,'1','01','2','02')");//ǩ������,��ǩ����ǩ
		map.put("acpt_typ", "decode(acpt_type,'1','01','2','02')");//ǩ������,��ǩ����ǩ
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//�Ƿ�ʹ������
		map.put("lmt_cont_no", "limit_ma_no");//����Э����
		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("way_manage", "decode(acpt_treat_mode,'1','1','2','2')");//�жҰ���ʽ,1ȫ�� 2����
		map.put("appl_ccy", "'CNY'");//�������
		map.put("appl_amt", "cont_amt");//����ǩ����Ԫ��
		map.put("cover_pct", "security_money_rt");//��֤�����
		map.put("cover_amt", "security_money_amt");//��֤���Ԫ��
		map.put("proc_fee_percent", "fee/10000");//��������
		map.put("proc_fee_amt", "cont_amt*fee/10000");//�����ѽ��
		map.put("risk_open_rt", "proc_fee_percent");//���ڳ�ŵ����
		map.put("risk_open_amt", "proc_fee_amount");//���ڳ�ŵ�ѽ�Ԫ��
		map.put("overdue_rt", "overdue_rate/10000");//����շ�Ϣ��
		map.put("dc_no", "serno");//�������
		map.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map.put("dirt_name", "direction_name");//Ͷ������
		map.put("dirt_name", "direction_name");//Ͷ������
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("acpt_bank_typ", "decode(acpt_bank_type,'1','2','2','3','3','4','4','5','5','6')");//�ж�������
		map.put("acpt_bank_id", "acpt_bank_id");//�ж����к�
		map.put("acpt_bank_name", "acpt_bank_name");//�ж�������
		map.put("acpt_use", "acpt_use");//�ж���;
		map.put("remarks", "loan_type_ext2");//��ע
		map.put("remarks", "loan_type_ext2");//��ע
		map.put("iqp_cde", "serno");//��������
		
		map.put("change_dt", "''");//��ͬ�������,����
		map.put("cont_name", "''");//��ͬ�������,����

		
		
		
		map.put("opr_usr", "cust_mgr");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cust_mgr");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cust_mgr");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cust_mgr");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//Э��״̬
		map.put("wf_appr_sts", "'997'");//����״̬��ȫ��Ϊͨ��
		
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
			String de_cus_corp=" delete from ctr_accp_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_accp_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_accp_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//����10���ѽ�����������ŷ�ʽΪ���ʵ���
			PreparedStatement up_limit_ind_ps=null;
			String up_limit_ind=" UPDATE ctr_accp_cont a SET a.limit_ind='1' WHERE a.cont_no IN ('D86018201500021','D86018201500022','D86018201600003','Y86018201500172','Y86508201400004','Y88038201400124','Y88038201400186','Y88038201500011','Y88088201500081','Y88088201500098') ";
			up_limit_ind_ps=conn.prepareStatement(up_limit_ind);
			up_limit_ind_ps.execute();
			if(up_limit_ind_ps!=null){
				up_limit_ind_ps.close();
			}
			
			//��������Э���Э����
			PreparedStatement up_coopr_usr_ps=null;
			String up_coopr_usr=" UPDATE ctr_accp_cont a SET a.coopr_usr = ( SELECT b.investigator_id FROM iqp_accp_app@CMISTEST2101 B WHERE b.serno= a.serno) ";
			up_coopr_usr_ps=conn.prepareStatement(up_coopr_usr);
			up_coopr_usr_ps.execute();
			if(up_coopr_usr_ps!=null){
				up_coopr_usr_ps.close();
			}
			
			
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
	
}

