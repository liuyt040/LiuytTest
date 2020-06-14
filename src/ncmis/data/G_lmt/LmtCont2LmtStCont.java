package ncmis.data.G_lmt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * ����ͬҵ�ͻ�����Э����Ϣlmt_cont��lmt_st_cont
 * ��������̨�˱� lmt_cont_detail��lmt_st_acc
 * ���˸�������
 * 
 */
public class LmtCont2LmtStCont {

	private static List<String> list=null;
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
			testOcc(conn);
//			check(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//����ͬҵ����Э��
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("lmt_cont_no", "lmt_serno");//����Э���
		map.put("lmt_st_dc_no", "serno");//������ˮ��
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("same_org_typ", "'01'");//ͬҵ��������
		map.put("crd_app_typ", "'20'");//������������
		map.put("crd_type", "'10'");//�������ͣ�ѭ��
		map.put("lmt_ccy", "'CNY'");//����ҵ�����ͣ������
		map.put("crd_tot_amt", "crd_totl_sum_amt");//����Э����
		map.put("tnr_typ", "decode(term_type,'01','DDD','02','MMM','03','YYY')");//��������	
		map.put("lmt_tnr", "term");//����
		map.put("crd_str_dt", "start_date");//������ʼ����
		map.put("crd_end_dt", "expi_date");//���ŵ�������
		map.put("remarks", "remarks");//���������
		//**��ϵͳȱʧ�ֶΣ��Ƿ���Ŀ���ţ�������ʽ
//		map.put("project_lmt_flag", "decode(prj_lmt_flg,'1','Y','2','N','')");//�Ƿ���Ŀ����
//		map.put("GURT_TYP", "assure");//������ʽ
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("main_usr", "cus_manager");//���ܿͻ�����
		map.put("crt_dt", "update_date");//�Ǽ�����
		map.put("lmt_cont_sts", "decode(lmt_state,'00','00','01','01','02','03','03','02','04','02')");//Э��״̬		
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
			String de_cus_corp=" delete from lmt_st_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont@CMISTEST2101 b where length(b.cus_id)>10 )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE lmt_st_cont a SET crt_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=crt_usr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
						
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE lmt_st_cont a SET main_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=crt_usr) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_input_br_id!=null){
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����Թ��ͻ�����̨�˱�lmt_st_acc 
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("lmt_st_acc_no", "item_id");//���̨�ʱ��
		map.put("lmt_cont_no", "lmt_serno");//����Э����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("same_org_typ", "'01'");//��������
		map.put("crd_typ", "'10'");//ѭ��
		map.put("lmt_ccy", "'CNY'");//����
		map.put("crd_tot_amt", "crd_lmt");//�˹��������Ŷ�ȣ�Ԫ��
		map.put("out_amt", "outstnd_lmt");//���ý�Ԫ��
		map.put("avai_amt", "residual_lmt");//���ý�Ԫ��

//		map.put("tnr_typ", "decode(term_type,'01','003','02','002','03','001'");//��������
//		map.put("lmt_tnr", "01");//
		
		map.put("crd_str_dt", "start_date");//������ʼ��
		map.put("crd_end_dt", "expi_date");//���ŵ�����
		map.put("lmt_sts", "decode(lmt_state,'00','00','01','01','02','03','03','02','04','02')");//̨��״̬
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
			String de_cus_corp=" delete from lmt_st_acc where 1=1 ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont_details@CMISTEST2101 b where length(b.cus_id)>10  ) ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE lmt_st_acc a SET main_usr= (SELECT  main_usr FROM lmt_st_cont b WHERE a.LMT_CONT_NO=b.LMT_CONT_NO) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE lmt_st_acc a SET main_bch = (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.main_usr) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			
			
			//������Э��Ԥ�Ǽ�״̬��Ϊǩ��
			String up_lmt_cont_sts = " UPDATE lmt_st_cont a SET a.lmt_cont_sts='01'  WHERE a.lmt_cont_sts='00' ";
			PreparedStatement up_lmt_cont_sts_ps=null;
			System.out.println(up_lmt_cont_sts);
			up_lmt_cont_sts_ps=conn.prepareStatement(up_lmt_cont_sts);
			up_lmt_cont_sts_ps.execute();
			if(up_lmt_cont_sts_ps!=null){
				up_lmt_cont_sts_ps.close();
			}
			
			//������̨��Ԥ�Ǽ�״̬��Ϊǩ��
			String up_acc_sts = " UPDATE Lmt_st_Acc a SET a.lmt_sts='01'  WHERE a.lmt_sts='00' ";
			PreparedStatement up_acc_sts_ps=null;
			System.out.println(up_acc_sts);
			up_acc_sts_ps=conn.prepareStatement(up_acc_sts);
			up_acc_sts_ps.execute();
			if(up_acc_sts_ps!=null){
				up_acc_sts_ps.close();
			}
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_input_br_id!=null){
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	//����ͬҵ�ͻ�����������lmt_st_dc 
	public static void test3(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("LMT_ST_DC_NO", "serno");//������
		map.put("lmt_st_no", "serno");//������ˮ��
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("large_bank_no", "cus_id");//�к�
		map.put("same_org_sub_typ", "'110'");//ͬҵ����������
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("crd_app_typ", "'20'");//��������
		map.put("lmt_ccy", "'CNY'");//����
		map.put("crd_tot_amt", "crd_lmt");//�˹��������Ŷ�ȣ�Ԫ��
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
			String de_cus_corp=" truncate table lmt_st_dc ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_dc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont@CMISTEST2101 where length(cus_id)>10 ) ";
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
	
	//��������̨��ռ����ϸ��limit_occupy 
	/*��˳����ֲ
	 * ͬҵ������ϸ
	 * ͬҵ�ͻ�������ϸ
	 */
	//���´�����ֲ��ͬҵ���š�
	public static void testOcc(Connection conn) {
		/*ͬҵ��ռ���쳣���ݲ�ѯ��ɸѡ��ͬһ��ͬ�ж���ռ�ü�¼������
		 * SELECT cont_no,COUNT(*) FROM (SELECT * FROM lmt_journal_acc a WHERE a.details_type='001' AND length(a.cus_id)=10) GROUP BY cont_no  HAVING COUNT(*)>1;
		 */
		String de_cont=" DELETE FROM cont ";
		String in_loan=" INSERT INTO cont SELECT a.cont_no FROM ctr_disc_cont@CMISTEST2101 a WHERE a.cont_state='200' ";
		
		String de_occ=" DELETE FROM limit_occupy  a where a.occ_business_seq like '%TXHT%' ";
		String occ_001=" INSERT INTO limit_occupy(ID,limit_id,limit_item_id,occ_business_seq,occ_amt,occ_stage,state,cus_id,currency,create_time,lmt_blc,last_chg_dt,Instu_Cde,limit_type,crd_typ) " 
						+" SELECT '001'||b.cont_no||b.item_id,'',item_id,cont_no,outstnd_lmt,'1','1','','CNY','',outstnd_lmt,'','0000','1','20' FROM lmt_cont_rel_bank@CMISTEST2101 b WHERE b.cont_no IN ( SELECT cont_no FROM cont ) AND B.OUTSTND_LMT>0 ";//ռ��
		String occ_002="INSERT INTO limit_occupy(ID,limit_id,limit_item_id,occ_business_seq,occ_amt,occ_stage,state,cus_id,currency,create_time,lmt_blc,last_chg_dt,Instu_Cde,limit_type,crd_typ) " 
						+" SELECT '002'||b.cont_no||b.item_id,'',item_id,cont_no,restored_lmt,'1','4','','CNY','',outstnd_lmt-restored_lmt,'','0000','1','20' FROM lmt_cont_rel_bank@CMISTEST2101 b WHERE b.cont_no IN ( SELECT cont_no FROM cont ) AND B.OUTSTND_LMT>0 ";//�ָ�
		//ɾ��������Ϊ0������
		String de_occ_0=" DELETE FROM limit_occupy where occ_amt=0 ";
		
		try {
			PreparedStatement de_cont_ps=null;
			de_cont_ps=conn.prepareStatement(de_cont);
			de_cont_ps.execute();
			
			PreparedStatement in_loan_ps=null;
			System.out.println(in_loan);
			in_loan_ps=conn.prepareStatement(in_loan);
			in_loan_ps.execute();
			
			PreparedStatement de_occ_ps=null;
			System.out.println(de_occ);
			de_occ_ps=conn.prepareStatement(de_occ);
			de_occ_ps.execute();
			
			PreparedStatement occ_001_ps=null;
			System.out.println(occ_001);
			occ_001_ps=conn.prepareStatement(occ_001);
			occ_001_ps.execute();
			
			PreparedStatement occ_002_ps=null;
			System.out.println(occ_002);
			occ_002_ps=conn.prepareStatement(occ_002);
			occ_002_ps.execute();
			
			PreparedStatement de_occ_0_ps=null;
			System.out.println(de_occ_0);
			de_occ_0_ps=conn.prepareStatement(de_occ_0);
			de_occ_0_ps.execute();
			
			if(de_cont_ps!=null){
				de_cont_ps.close();
			}
			if(in_loan_ps!=null){
				in_loan_ps.close();
			}
			if(de_occ_ps!=null){
				de_occ_ps.close();
			}
			if(occ_001_ps!=null){
				occ_001_ps.close();
			}
			if(occ_002_ps!=null){
				occ_002_ps.close();
			}
			if(de_occ_0_ps!=null){
				de_occ_0_ps.close();
			}
			/*ɸѡռ����ϸ��ָ���ϸʱ����ͬ�����ݣ���Ӱ�����ҵ��
			 *1 SELECT a.occ_business_seq,a.create_time,b.create_time FROM (SELECT * FROM limit_occupy a WHERE a.state='1' ) a
JOIN (SELECT * FROM limit_occupy a WHERE a.state='4' ) b
ON a.occ_business_seq=b.occ_business_seq WHERE a.create_time=b.create_time
			 * 2 �鿴����ʱ��Ϊ�յ�����
			 */
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	public static void check(Connection conn){
		//У������Э�������̨�˽���Ƿ�һ��
		String check_amt=" SELECT a.lmt_cont_no,b.lmt_cont_no,a.crd_tot_amt,b.crd_tot_amt FROM lmt_st_cont a "
						+" LEFT JOIN lmt_st_acc b ON a.lmt_cont_no=b.lmt_cont_no "
						+" WHERE a.crd_tot_amt!=b.crd_tot_amt ";
		
		//У��ռ����ϸ���������̨����һ�µ�����
		String check_out=" SELECT lmt_st_acc_no,e.out_amt,f.* FROM lmt_st_acc e " 
						+" LEFT JOIN ( "
						+" SELECT c.*,d.occ_amt_2,c.occ_amt_1-decode(d.occ_amt_2,NULL,0,d.occ_amt_2) out_amt1  FROM ( "
						+" SELECT a.limit_item_id,a.state,SUM(a.occ_amt) occ_amt_1 FROM limit_occupy a WHERE a.state='1' GROUP BY a.limit_item_id,a.state " 
						+" ) c LEFT JOIN (SELECT b.limit_item_id,b.state,SUM(b.occ_amt) occ_amt_2 FROM limit_occupy b WHERE b.state='4' GROUP BY b.limit_item_id,b.state ) d ON c.limit_item_id = d.limit_item_id "
						+" ) f ON lmt_st_acc_no=f.limit_item_id WHERE out_amt!=out_amt1" ;
		
		//���ͬһ��ҵ��ռ��ʱ����ָ�ʱ����ͬ�����ݣ���Ӱ�������Ȼָ�
		String check_create_time="SELECT a.limit_item_id,a.occ_business_seq,a.create_time,b.create_time FROM (SELECT * FROM limit_occupy a WHERE a.state='1' ) a "
								+" JOIN (SELECT * FROM limit_occupy a WHERE a.state='4' ) b "
								+" ON a.occ_business_seq=b.occ_business_seq and a.limit_item_id=b.limit_item_id WHERE a.create_time=b.create_time";
		
		PreparedStatement check_amt_ps=null;
		PreparedStatement check_out_ps=null;
		PreparedStatement check_create_time_ps=null;
		try {
			check_amt_ps=conn.prepareStatement(check_amt);
			ResultSet rs=check_amt_ps.executeQuery();
			System.out.println("*********��ʼУ�飺У��ͬҵ�ͻ�����Э��������̨���ܽ���������*********");
			while(rs.next()){
				System.out.println(rs.getObject(1)+" "+rs.getObject(2)+" "+rs.getObject(3)+" "+rs.getObject(4));
			}
			System.out.println("*********����У�飺У��ͬҵ�ͻ�����Э��������̨���ܽ���������*********");
			if(rs!=null){
				rs.close();
			}
			
			check_out_ps=conn.prepareStatement(check_out);
			ResultSet rs1=check_out_ps.executeQuery();
			System.out.println("*********��ʼУ�飺У��ͬҵ�ͻ�ռ����ϸռ�ý��������̨��ռ�ý���������*********");
			while(rs1.next()){
				System.out.println("����̨�ˣ�"+rs1.getObject(1)+"��̨��ռ�ý� "+rs1.getObject(2)+"�� ��ϸͳ�Ƶ�ռ�ý�"+rs1.getObject(7));
			}
			System.out.println("*********����У�飺У��ͬҵ�ͻ�ռ����ϸռ�ý��������̨��ռ�ý���������*********");
			if(rs1!=null){
				rs1.close();
			}
			
			check_create_time_ps=conn.prepareStatement(check_create_time);
			ResultSet rs2=check_create_time_ps.executeQuery();
			System.out.println("*********��ʼУ�飺У��ͬҵ�ͻ�����ʹ����ϸ����ͬһ������ռ�ü�¼��ָ���¼����ʱ����ͬ������*********");
			while(rs2.next()){
				System.out.println("̨�˱�ţ�"+rs2.getObject(1)+"ҵ���ţ�"+rs2.getObject(2)+"������ʱ�䣺 "+rs2.getObject(3)+"���ָ�ʱ�� "+rs2.getObject(4));
			}
			System.out.println("*********����У�飺��У��ͬҵ�ͻ�����ʹ����ϸ����ͬһ������ռ�ü�¼��ָ���¼����ʱ����ͬ������*********");
			if(rs2!=null){
				rs2.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
			if(check_amt_ps!=null){
				try {
					check_amt_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(check_out_ps!=null){
				try {
					check_out_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(check_create_time_ps!=null){
				try {
					check_create_time_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
