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
 * ����Э�� ctr_itsmstout_cont 2 ctr_loc_cont
 */
public class CtrItsmstoutCont2CtrLocCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM Ctr_ItSmStout_Cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_ItSmStout@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
		map.put("cont_str_dt", "apply_start_date");//Э��ǩ������
		map.put("cont_end_dt", "apply_end_date");//��ͬ������
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("iqp_cde", "serno");//��������
		map.put("dc_no", "serno");//�������
		map.put("cl_typ", "'LC'");//�Ŵ�Ʒ��
		map.put("prd_cde", "decode(biz_type,'420001','LC02','420003','LC01')");//LC02���ڿ�֤��LC01����֤��֤
		map.put("prd_name","decode(biz_type,'420001','��������֤','420003','��������֤')");//��Ʒ����
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//�Ƿ�ʹ������
		map.put("cl_typ_sub", "'LC'");//�Ŵ�Ʒ��ϸ��
		map.put("limit_ma_no", "limit_ma_no");//����Э����
		map.put("limit_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("way_manage", "decode(limit_ind,'1','1','2','2')");//����֤����ʽ,ȫ����
		map.put("appl_ccy", "apply_cur_type");//�������
		map.put("appl_amt", "open_amt");//��֤���
		map.put("exc_rt", "exchange_rate");//����
		map.put("appl_amt_cny", "open_amt*exchange_rate");//������������ң�Ԫ��
		map.put("loc_pay_term", "''");//����֤��������
		map.put("sol_rt", "over_ship_rate/100");//��װ����
		map.put("locdt_type", "decode(lc_type,'001','01','002','02')");//����֤��������,01���ڣ�02Զ��
		map.put("fast_day", "''");//Զ������
		map.put("ioc_dt", "apply_end_date");//����֤��Ч��
		map.put("max_amt", "open_amt*exchange_rate*(1+over_ship_rate/100)");//����֤���Ԫ��
		map.put("cover_ccy", "security_cur_type");//��֤�����
		map.put("cover_rt", "'1'");//��֤�����
		map.put("cover_pct", "security_money_rt");//��֤�����
		map.put("cover_amt", "security_money_amt");//��֤���Ԫ��
		map.put("crt_end_dt", "''");//����֤���װ������
		map.put("cover_amt_cny", "security_rmb_amount");//��֤���ۺ�����ҽ�Ԫ��
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("cont_typ", "'01'");//��ͬ���ͣ���߷�����
		
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
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
			String de_cus_corp=" delete from ctr_loc_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_loc_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_itsmstout_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_loc_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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

