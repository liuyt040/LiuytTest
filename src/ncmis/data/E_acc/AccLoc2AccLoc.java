package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccLoc2AccLoc {
	
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

	//��������̨֤��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//̨�ʱ��
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("appl_ccy", "apply_cur_type");//�������
		map.put("appl_amt", "open_amt");//��֤���
		map.put("appl_amt_cny", "open_amt*exchange_rate");//������������ң�Ԫ��
		map.put("loc_pay_term", "''");//����֤��������
		map.put("fast_day", "''");//Զ������
		map.put("ioc_dt", "end_date");//����֤��Ч��
		map.put("max_amt", "open_amt*exchange_rate*(1+over_ship_rate/100)");//����֤���Ԫ��
		map.put("cover_ccy", "security_cur_type");//��֤�����
		map.put("cover_rt", "'1'");//��֤�����
		map.put("cover_pct", "security_money_rt");//��֤�����
		map.put("cover_amt", "security_money_amt");//��֤���Ԫ��
		map.put("cover_amt_cny", "security_rmb_amount");//��֤���ۺ�����ҽ�Ԫ��
		map.put("acc_sts", "decode(account_status,'0','1','1','2','6','3','9','4','7','4')");//̨��״̬
		map.put("loc_no", "accp_no");//����֤���
		map.put("cla", "cla");//�弶����
		
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prd_cde", "decode(biz_type,'420001','LC02','420003','LC01')");//LC02���ڿ�֤��LC01����֤��֤
		map1.put("prd_name","decode(biz_type,'420001','��������֤','420003','��������֤')");//��Ʒ����
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map1.put("exc_rt", "exchange_rate");//����
		map1.put("sol_rt", "over_ship_rate/100");//��װ����
		map1.put("limit_ma_no", "limit_ma_no");//����Э����
		map1.put("limit_acc_no", "limit_acc_no");//���̨�ʱ��
	
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "input_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "input_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "input_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "input_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
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
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table acc_loc  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_loc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_itsmstout@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_acc_loc_ps=null;
			String up_acc_loc=" update acc_loc a set  ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_itsmstout_cont@CMISTEST2101 b  where b.cont_no=a.cont_no )";
			System.out.println(up_acc_loc);
			up_acc_loc_ps=conn.prepareStatement(up_acc_loc);
			up_acc_loc_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_loc a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_loc_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_loc b  )";
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
