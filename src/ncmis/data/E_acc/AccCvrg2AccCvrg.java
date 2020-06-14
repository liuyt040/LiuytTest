package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * ����Э�� acc_cvrg 2 acc_cvrg
 */
public class AccCvrg2AccCvrg {

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

	//���뱣��Э��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//̨�ʱ��
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("cvrg_no", "bill_no");//�������
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("acc_sts", "decode(account_status,'0','1','1','2','3','4','6','3','9','4')");//̨��״̬
		map.put("cont_start_dt", "start_date");//������ʼ����
		map.put("pre_due_dt", "due_date");//������ֹ����
		map.put("stpint_dt", "start_date");//������������
		map.put("appl_ccy", "'CNY'");//�������
		map.put("appl_amt", "guarantee_amount");//������
		map.put("cover_pct", "security_money_rt");//��֤�����
		map.put("cover_amt", "security_money_amt");//��֤���Ԫ��
		map.put("day_rt", "day_rate/100");//��֤���Ԫ��
		map.put("exc_rt", "'1'");//����
		map.put("cla", "cla");//�弶����
		//��ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("dc_no", "serno");//�������
		map1.put("prd_cde", "decode(substr(guarantee_type,1,1),'1','CVR002','2','CVR001')");//CVR001���ʱ�����CVR002������
		map1.put("prd_name", "decode(substr(guarantee_type,1,1),'1','�����Ա���','2','�������Ա���')");//��Ʒ����
		StringBuffer sb = new StringBuffer("");
		sb.append("'11','0102020',");
		sb.append("'12','0102030',");
		sb.append("'13','0102010',");
		sb.append("'14','0102040',");
		sb.append("'15','0102091',");
		sb.append("'16','0102050',");
		sb.append("'17','0102094',");
		sb.append("'18','0102080',");
		sb.append("'19','0102094',");
		sb.append("'1A','0102094',");
		sb.append("'21','0101010',");
		sb.append("'22','0101050',");
		sb.append("'23','0101060',");
		sb.append("'24','0102094',");
		sb.append("'25','0101070'");
		map1.put("cvrg_type", "decode(guarantee_type,"+sb.toString()+")");//��������
		map1.put("cvrg_deal_type", "decode(limit_ind,'1','1','2','2')");//��������
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map1.put("proc_fee_amt", "proc_fee_amount");//����������(Ԫ
		map1.put("guar_fee_amt", "lg_fee_amount");//����������(Ԫ)
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map1.put("dirt_name", "direction_name");//Ͷ������

		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
		map.put("main_bch", "input_br_id");//���ܻ���
//		map.put("iqp_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		
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
			String de_cus_corp=" truncate table acc_cvrg   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_cvrg ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_cvrg@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_cvrg a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_cvrg a set  ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101 b where b.cont_no=a.cont_no )";
			System.out.println(up_cont);
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_cvrg_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_cvrg b  )";
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

