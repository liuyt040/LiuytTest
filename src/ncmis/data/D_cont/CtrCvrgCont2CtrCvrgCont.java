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
 * ����Э�� ctr_cvrg_cont 2 ctr_cvrg_cont
 */
public class CtrCvrgCont2CtrCvrgCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_cvrg_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_cvrg@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
	//���뱣��Э��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("cont_str_dt", "start_date");//Э��ǩ������
		map.put("cont_end_dt", "due_date");//��ͬ������
		map.put("iqp_cde", "serno");//��������
		map.put("cl_typ", "'CVRG'");//�Ŵ�Ʒ��
		map.put("prd_cde", "decode(substr(guarantee_type,1,1),'1','CVR002','2','CVR001')");//CVR001���ʱ�����CVR002������
		map.put("prd_pk", "decode(substr(guarantee_type,1,1),'1','FFFBA8BA014DDACC9FD24FDDF9ED102B','2','FFFBA8BA00AD08DE9FD072921FC660FF')");//CVR001���ʱ�����CVR002������
		map.put("prd_name", "decode(substr(guarantee_type,1,1),'1','�������Ա���','2','�����Ա���')");//��Ʒ����
		
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
		map.put("cvrg_type", "decode(guarantee_type,"+sb.toString()+")");//��������
		map.put("cont_typ", "'01'");//Э������
		map.put("way_manage", "decode(limit_ind,'1','1','2','2')");//��������ʽ,ȫ����
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//�Ƿ�ʹ������
		map.put("lmt_cont_no", "limit_ma_no");//����Э����
		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("appl_ccy", "'CNY'");//�������
		map.put("appl_amt", "cont_amt");//����ǩ����Ԫ��
		map.put("loan_str_dt", "start_date");//������ʼ��
		map.put("due_dt", "due_date");//������������
		map.put("due_dt", "due_date");//������������
		map.put("cover_pct", "security_money_rt");//��֤�����
		map.put("cover_amt", "security_money_amt");//��֤���Ԫ��
		map.put("proc_fee_amt", "proc_fee_amount");//����������(Ԫ
		map.put("guar_fee_amt", "lg_fee_amount");//����������(Ԫ)
		map.put("exc_rt", "'1'");//����
		map.put("appl_amt_cny", "cont_amt");//������������ң�Ԫ��
		map.put("cover_ccy", "'CNY'");//��֤�����
		map.put("cover_rt", "'1'");//��֤�����
		map.put("cover_amt_cny", "security_money_amt");//��֤���ۺ�����ҽ��(Ԫ)
		map.put("dc_no", "serno");//�������
		map.put("corep_no", "corep_no");//��ͬЭ���
		map.put("corep_amt", "trade_cont_amt");//��غ�ͬ���
		map.put("bfcy_name", "bfcy_name");//����������
		map.put("bfcy_no", "''");//�������˻�
		map.put("honour_des", "honour_des");//�����и�����˵��
		map.put("guarantee_pay_way", "decode(guarantee_pay_way,'01','00','02','01')");//���ʽ���������������������
		map.put("remarks", "cla_suggestion");//���ʽ���������������������
		map.put("dirt_cde", "loan_direction");//����Ͷ�����dirt_cde
		map.put("dirt_name", "direction_name");//Ͷ������
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		
	
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
			String de_cus_corp=" delete from ctr_cvrg_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_cvrg_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_cvrg_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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

