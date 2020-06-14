package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * ����Э�� acc_disc 2 acc_disc
 */
public class AccDisc2AccDisc {

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

	//��������̨��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//̨�ʱ��
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("acc_sts", "decode(account_status,'00','1','10','2','20','4','21','4')");//̨��״̬
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("change_dt", "''");//��ͬ�������,���� 
		map.put("cl_typ", "'03'");//�Ŵ�Ʒ��
		map.put("cla", "cla");//�弶����
		map.put("appl_amt", "bill_amount");//Ʊ���Ԫ��
		map.put("appl_ccy", "'CNY'");//�������
		map.put("draft_no", "draft_no");//��Ʊ����
		map.put("bill_str_dt", "bill_issuing_date");//Ʊ��ǩ����
		map.put("bill_end_dt", "bill_due_day");//Ʊ�ݵ�����
		map.put("not_hegotiable_flg", "not_hegotiable_flg");//����ת�ñ��
		map.put("bill_issue", "bill_issue");//Ʊ��ǩ����
		map.put("interest", "disc_interest");//����Ϣ
		map.put("paymoney", "cash_amount");//ʵ�����
//		map.put("settl_date", "latest_date");//��������
		//�Ӻ�ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("serno", "serno");//ҵ����ˮ��
		map1.put("dc_no", "serno");//������
		map1.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//Э��״̬
		map1.put("sign_dt", "sign_date");//ǩ������
		map1.put("prd_cde", "decode(bull_type,'1','01021314','2','01021314')");//01021314��Ʊ��01021314��Ʊ
		map1.put("prd_name", "'����'");//1һ��жң�2���Ҳ�
		map1.put("cl_typ_sub", "decode(bull_type,'1','01','2','02')");//01��Ʊ��02��Ʊ
		map1.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//�Ƿ�ʹ������
		map1.put("cont_name", "'����Э��'");//Э������
		map1.put("lmt_cont_no", "limit_ma_no");//����Э����
		map1.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map1.put("lr_ind", "decode(limit_ind,'1','Y','2','N')");//�Ƿ�ͷ���ҵ��
		map1.put("bill_check_ind", "decode(biz_type,'010002','Y','010001 ','N')");//�Ƿ����Ʊ��
		map1.put("dis_check_ind", "decode(if_tc,'1','Y','2','N')");//�Ƿ��������
		map1.put("cus_name", "cus_name");//�ͻ�����
		map1.put("cus_id", "cus_id");//�ͻ����
		map1.put("cont_typ", "'01'");//��ͬ����
		map1.put("dis_typ", "decode(disc_type,'1','01','2','02')");//���ַ�ʽ,01��ϣ�02���
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map1.put("dirt_name", "direction_name");//Ͷ������
		map1.put("usg_dsc", "use_dec");//��;
		map1.put("agr_typ", "agriculture_type");//��ũ���,��ֵһ��
		map1.put("agr_use", "decode(agriculture_use,'1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260')");//��ũ���,��ֵһ��
//		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
//		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
//		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("agent_dis_ind", "''");//�Ƿ��������
			
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "disc_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "disc_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "disc_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "disc_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
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
			String de_cus_corp=" truncate table acc_disc ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_disc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_disc@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_disc a set ("+keys1.toString().substring(1)+") =(SELECT "+values1.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101 b where b.cont_no= a.cont_no)";
			System.out.println(up_cont);
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_disc a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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
			String delte_ctr_loan_cont=" DELETE FROM ctr_dis a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_disc b  ) ";
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

