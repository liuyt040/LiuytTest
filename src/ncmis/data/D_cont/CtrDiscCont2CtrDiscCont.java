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
 * ����Э�� ctr_disc_cont 2 ctr_disc_cont
 */
public class CtrDiscCont2CtrDiscCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_disc_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_disc@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("cont_name", "'����Э��'");//Э������
		map.put("sign_dt", "sign_date");//ǩ������
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("iqp_cde", "serno");//��������
		map.put("dc_no", "serno");//������
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("prd_cde", "decode(bull_type,'1','01021314','2','01021314')");//01021314��Ʊ��01021314��Ʊ
		map.put("prd_name", "'����'");//1һ��жң�2���Ҳ�
		map.put("cl_typ_sub", "decode(bull_type,'1','01','2','02')");//01��Ʊ��02��Ʊ
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//�Ƿ�ʹ������
		map.put("lmt_cont_no", "limit_ma_no");//����Э����
		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("cl_typ", "'03'");//�Ŵ�Ʒ��
		map.put("lr_ind", "decode(limit_ind,'1','Y','2','N')");//�Ƿ�ͷ���ҵ��
		map.put("bill_check_ind", "decode(biz_type,'010002','Y','010001 ','N')");//�Ƿ����Ʊ��
		map.put("dis_check_ind", "decode(if_tc,'1','Y','2','N')");//�Ƿ��������
		map.put("cont_typ", "'01'");//��ͬ����
		map.put("counterfoil_num", "bill_qty");//Ʊ������
		map.put("face_amt_sum", "par_amount");//Ʊ���ܽ�Ԫ��
		map.put("counterfoil_ccy", "'CNY'");//Ʊ�ݱ���
		map.put("dis_dt", "sign_date");//��������
		map.put("loan_str_dt", "sign_date");//�����������
		map.put("dis_ir", "discount_ir*12");//����������
		map.put("pay_int_typ", "decode(pay_mode,'1','2','2','1','3','3','4','4')");//��Ϣ��ʽ
		map.put("is_pay_cust_inner_bank", "decode(is_pay_cust_inner_bank,'1','Y','2','N')");//�򷽸�Ϣ���Ƿ������п���
		map.put("is_third_cust_inner_bank", "decode(is_third_cust_inner_bank,'1','Y','2','N')");//��������Ϣ���Ƿ������п���
		map.put("third_cust_name", "third_cust_name");//��������Ϣ������
		map.put("third_bank_no", "third_bank_no");//��������Ϣ�˿������к�
		map.put("third_cust_no", "third_cust_no");//��������Ϣ�˿ͻ���
		map.put("third_acct_no", "third_acct_no");//��������Ϣ���˺�
		map.put("third_pay_rate", "third_pay_rate");//��������Ϣ����
		map.put("dis_typ", "decode(disc_type,'1','01','2','02')");//���ַ�ʽ,01��ϣ�02���
		map.put("online_mark", "decode(online_mark,'1','Y','2','N')");//�����������
		map.put("agent_dis_ind", "''");//�Ƿ��������
		map.put("agent_dis_ind", "''");//�Ƿ��������
		map.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map.put("dirt_name", "direction_name");//Ͷ������
		map.put("usg_dsc", "use_dec");//��;
		map.put("agr_typ", "agriculture_type");//��ũ���,��ֵһ��
		map.put("agr_use", "decode(agriculture_use,'1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260')");//��ũ���,��ֵһ��
//		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
//		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
//		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		

		map.put("change_dt", "''");//��ͬ�������,���� 
		map.put("cont_amt", "''");//Э���ܽ����ã���Ʊ���ܽ��Ϊ׼
			
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
			String de_cus_corp=" delete from ctr_dis where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_dis ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_dis a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
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

