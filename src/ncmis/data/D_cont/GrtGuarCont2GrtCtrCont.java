package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * ������ͬ��Ϣ grt_guar_cont 2 grt_ctr_cont
 */
public class GrtGuarCont2GrtCtrCont {

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

	//���뵣����ͬ��grt_ctr_cont
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','20')");//����
		map.put("grt_cont_no", "guar_cont_no");//������ͬ���
//		map.put("grt_cont_no_chinese", "''");//������ͬ���ı��
		map.put("grt_cont_typ", "decode(guar_cont_type,'1','2','2','1')");//������ͬ����
		map.put("gurt_typ", "decode(guar_way,'10000','10','20000','20','30001','30','30002','30','30003','30')");//������ʽ
		map.put("guee_type", "decode(guar_way,'30001','1','30002','3','30003','2','')");//��֤������ʽ
		/*
		//****
		
		**/
		map.put("cus_id", "borrower_no");//�������˿ͻ����
		map.put("cus_name", "borrower_name");//�������˿ͻ�����
		
		map.put("guar_cus_id", "guar_no");//�����˱��
		map.put("guar_cus_name", "guar_name");//����������
		map.put("guar_cert_typ", "decode(cer_type,'10','10100','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//������֤������
		map.put("guar_cert_cde", "cer_no");//������֤������
		map.put("gurt_ccy", "'CNY'");//��������
		map.put("gurt_amt", "guar_amt");//������Ԫ��
		map.put("sign_dt", "sign_date");//ǩ������
		map.put("cont_eff_dt", "guar_start_date");//������ͬ��Ч��
		map.put("cont_end_dt", "guar_end_date");//������ͬ������
		map.put("other_describe", "notes");//�����ر�Լ��
		
		
		map.put("crt_usr", "input_id");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("last_chg_usr", "input_id");//������
		map.put("crt_dt", "regi_date");//�Ǽ�����
		map.put("main_usr", "customer_mgr");//���ܿͻ�����	
		map.put("main_bch", "main_br_id");//���ܻ���
		
		
		map.put("grt_cont_sts", "decode(guar_cont_state,'100','00','101','10','103','90','104','41','109','00')");//Э��״̬
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
			String de_cus_corp=" truncate table grt_ctr_cont ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO grt_ctr_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_guar_cont@CMISTEST2101  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
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
	
	//���뵣����ͬ��ѺƷ��ϵ�� grt_guaranty_re 2 GRT_GRTCTR_GUAR_REL 
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("pk_id", "dbms_random.string('l','32')");//����
		map.put("GURT_CONT_NO", "guar_cont_no");//������ͬ���
		map.put("gurt_id", "decode(gage_way,'10000',guaranty_id,'20000',guaranty_id,'')");//ѺƷ���,��֤�ൣ����ʽ��ѺƷ���ҪΪ�գ�������ϵͳ�������У�飬Ӱ���������
		map.put("Gurt_Typ", "decode(gage_way,'10000','10','20000','20','30')");//������ʽ
		map.put("Gurt_Ccy", "'CNY'");//����
//		map.put("cus_id", "''");//�����˱��
//		map.put("cus_name", "''");//����������
//		map.put("cert_typ", "''");//������֤������
//		map.put("cert_code", "''");//������֤������
		
		map.put("Gurt_Amt", "GUARANTY_AMT");//�������
		map.put("Rel_Sts", "'2'");//ѺƷ״̬
//		map.put("VCH_CUS_ID", "''");//�������˿ͻ���
		map.put("VCH_CUS_NAME", "''");//������������
		
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
			String de_cus_corp=" truncate table GRT_GRTCTR_GUAR_REL ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO GRT_GRTCTR_GUAR_REL ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_guaranty_re@CMISTEST2101 WHERE 	guar_cont_no IS NOT NULL  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement insert_gurt_amt_ps=null;
			String insert_gurt_amt=" UPDATE GRT_GRTCTR_GUAR_REL a SET a.gurt_amt=( SELECT gurt_amt FROM grt_ctr_cont b WHERE b.grt_cont_no=a.gurt_cont_no ) WHERE a.gurt_typ='30' ";
			System.out.println(insert_gurt_amt);
			insert_gurt_amt_ps=conn.prepareStatement(insert_gurt_amt);
			insert_gurt_amt_ps.execute();
			if(insert_gurt_amt_ps!=null){
				insert_gurt_amt_ps.close();
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
	
	//����ѺƷ����grt_g_basic_info��Ѻ grt_p_basic_info��Ѻ    2 t_guar_base_info 
	public static void test3(Connection conn) {
		
		//��Ѻ
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "GUARANTY_ID");//ѺƷ���
		map.put("GUAR_TYPE_CD", "gage_type");//�����������
		map.put("guar_name", "gage_name");//ѺƷ����
		map.put("GUAR_TYPE_CD_CNNAME", "'1'");//������������
		map.put("GUAR_BUSISTATE", "'07'");//ѺƷ����ҵ��״̬��ȫ��Ϊ���ݹ���
		map.put("eval_amt", "max_mortagage_amt");//����Ѻ��ֵ��Ԫ
		map.put("EVAL_CURRENCY", "'CNY'");//ȷ�ϼ�ֵ����
		map.put("USE_AMT", "max_mortagage_amt");//ռ�ö��
		map.put("REMAIN_AMT", "0");//ʣ����
		map.put("GUAR_CUS_ID", "cus_id");//ѺƷ�����˱��
		map.put("GUAR_CUS_NAME", "cus_name");//ѺƷ����������
		map.put("GUAR_CUS_TYPE", "decode(cus_typ,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");//ѺƷ����������
		map.put("GUAR_CERT_TYPE", "decode(cer_type,'10','10100','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//ѺƷ������֤������
		map.put("GUAR_CERT_CODE", "cer_no");//ѺƷ������֤������
		map.put("COMMON_ASSETS_IND", "'N'");//�Ƿ��вƲ�
		map.put("OCCUPYOFOWNER", "1.000000");//ѺƷ��������ռ�ݶ�
		map.put("INSURANCE_IND", "''");//�Ƿ���Ҫ������
		map.put("SALE_STATE", "''");//����ѺƷ��������
		map.put("INSUR_STATE", "decode(assurance_flg,'1','02','2','01','')");//���հ���״̬
		map.put("COVER_INFO", "''");//���ն�ծ��ĸ������
		map.put("GUAR_CREATE_DATE", "INPUT_DATE");//����ʱ��
		map.put("GUAR_LASTUPDATE_DATE", "INPUT_DATE");//������ʱ��
		map.put("CREATE_USERID", "CREATE_USER_NO");//������
		map.put("CREATE_ORGID", "INPUT_BR_ID");//��������
		map.put("LASTMODIFY_USERID", "CREATE_USER_NO");//����޸���
		map.put("LASTMODIFY_ORGID", "INPUT_BR_ID");//����޸��˻���
		map.put("CREATE_SOURCE", "'01'");//ѺƷ������Դ
		map.put("OTHER_BACK_GUAR_IND", "''");//�����Ƿ����趨����Ȩ
		map.put("MYBACK_GUAR_FIRST_SEQ", "''");//���е���Ȩ�����ܳ�Ȩ˳��
		map.put("CERTI_COMPLETE_STATUS", "''");//Ȩ֤�걸״̬���Ƿ�ȫ������ʵ��
		map.put("EVAL_DATE", "INPUT_DATE");//����ȷ������
		map.put("GUAR_IMPLEMENTED_IND", "''");//��֤�Ƿ�����ʵ
		map.put("OLD_GUAR_NO", "''");//ԭѺƷ���
		map.put("REMARK", "''");//��ע��Ϣ
		map.put("REG_STATE", "''");//ѺƷ�Ǽǰ���״̬**
		map.put("IS_REAL", "''");//�����ֶ�
		map.put("CUST_MGR", "CREATE_USER_NO");//�ܻ���
		map.put("MAIN_BR_ID", "INPUT_BR_ID");//�������
		map.put("GUAR_ACCOUNT_STATE", "''");//ѺƷ����״̬**
		map.put("IS_POSITIVE_CORRELA", "''");//�Ƿ�ʵ�������
		map.put("CK_LOAN_NO", "''");//�����Ȩ��
		map.put("LOCK_STATE", "''");//ѺƷ����״̬���Ƿ���;����01����02������
		map.put("IS_STOCK", "''");//�Ƿ�������ݣ�Y�� N��
		map.put("BILL_STATUS", "''");//�Ŵ�Ʊ��״̬
		map.put("GUAR_LOCATION", "area_location");//������ص�λ��
		map.put("GUARANTY_BASE_AMT", "''");//ѺƷ��ֵ
		map.put("GUARANTY_RATE", "mortagage_rate");//ѺƷ����Ѻ��
		map.put("GUARANTY_BASE_CNY", "''");//ѺƷ����
		
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			if(!"GUAR_LOCATION".equals(entry.getKey())){
				keys1.append(","+entry.getKey());
				values1.append(","+entry.getValue());
			}
			
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		try {
			
			PreparedStatement de_grt_turn_ps=null;
			String de_grt_turn=" truncate table grt_turn  ";
			de_grt_turn_ps=conn.prepareStatement(de_grt_turn);
			de_grt_turn_ps.execute();
			if(de_grt_turn_ps!=null){
				de_grt_turn_ps.close();
			}
			
			Statement stat = conn.createStatement();
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10036', 'ZY0301999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10037', 'ZY0302001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10038', 'ZY0303999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10039', 'ZY0304999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10027', 'ZY1299999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10028', 'ZY1206001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10029', 'ZY1101001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10034', 'ZY1104001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10035', 'ZY1199999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10032', 'ZY1001001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10033', 'ZY1301001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10020', 'ZY0102001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10019', 'ZY1205001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10026', 'ZY1402001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10010', 'ZY1201001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10030', 'DY0703999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10002', 'DY0599999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10003', 'DY0101001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10004', 'DY0701001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10005', 'DY0601001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10006', 'DY0702999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10007', 'DY0802001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10008', 'DY9999999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10009', 'DY9999999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10011', 'ZY0201002')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10012', 'ZY0399999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10013', 'ZY0699999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10014', 'ZY0401004')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10015', 'ZY1203002')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10017', 'ZY0101999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10018', 'ZY0901001')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10021', 'ZY9999999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10024', 'ZY0401004')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10025', 'ZY0401005')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10040', 'ZY0701002')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10022', 'ZY9999999')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10031', 'DY0401002')");
			stat.addBatch(" insert into grt_turn (OLD_GRT, NEW_GRT) values ('10001', 'DY0402999')");
			stat.executeBatch();
			
			
			PreparedStatement de_ps_cus_corp=null;
//			String de_cus_corp=" delete from t_guar_base_info where 1=1 and guar_type_cd in (SELECT a.guar_type_cd FROM t_guar_class_info a WHERE a.guar_type_state='1' AND a.guar_way ='DY') ";
			String de_cus_corp=" delete from t_guar_base_info where 1=1 ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_guar_base_info_ps=null;
			String insert_guar_base_info=" INSERT INTO t_guar_base_info ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM grt_g_basic_info@CMISTEST2101  )";
			System.out.println(insert_guar_base_info);
			insert_guar_base_info_ps=conn.prepareStatement(insert_guar_base_info);
			insert_guar_base_info_ps.execute();
			if(insert_guar_base_info_ps!=null){
				insert_guar_base_info_ps.close();
			}
			conn.commit();
			
			PreparedStatement insert_guar_base_info_2_ps=null;
			String insert_guar_base_info_2=" INSERT INTO t_guar_base_info ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM grt_p_basic_info@CMISTEST2101  )";
			System.out.println(insert_guar_base_info_2);
			insert_guar_base_info_2_ps=conn.prepareStatement(insert_guar_base_info_2);
			insert_guar_base_info_2_ps.execute();
			if(insert_guar_base_info_2_ps!=null){
				insert_guar_base_info_2_ps.close();
			}
			
			conn.commit();
			//����ѺƷ����
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE t_guar_base_info a SET a.guar_type_cd=( SELECT B.NEW_GRT FROM grt_turn b WHERE b.old_grt= A.GUAR_TYPE_CD ) WHERE a.guar_type_cd IN ( SELECT old_grt FROM grt_turn ) ";
			System.out.println(main_br_id);
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			conn.commit();
			//������Ҫ���⴦���Ҫ����
			//1��������-�ƽ�
			String guiJinShu1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0201001' WHERE a.guar_no IN (SELECT b.guaranty_id FROM Grt_P_Metal@CMISTEST2101 b WHERE b.metal_type='10001')";
			System.out.println(guiJinShu1);
			PreparedStatement guiJinShu1_ps=null;
			guiJinShu1_ps=conn.prepareStatement(guiJinShu1);
			guiJinShu1_ps.execute();
			if(guiJinShu1_ps!=null){
				guiJinShu1_ps.close();
			}
			conn.commit();
			//2���𡢰���������
			String guiJinShu2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0299999' WHERE a.guar_no IN (SELECT b.guaranty_id FROM Grt_P_Metal@CMISTEST2101 b WHERE b.metal_type IN ('10002','10003','10005'))";
			System.out.println(guiJinShu2);
			PreparedStatement guiJinShu2_ps=null;
			guiJinShu2_ps=conn.prepareStatement(guiJinShu2);
			guiJinShu2_ps.execute();
			if(guiJinShu2_ps!=null){
				guiJinShu2_ps.close();
			}
			conn.commit();
			//3�鱦
			String guiJinShu3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0201002' WHERE a.guar_no IN (SELECT b.guaranty_id FROM Grt_P_Metal@CMISTEST2101 b WHERE b.metal_type='10004')";
			System.out.println(guiJinShu3);
			PreparedStatement guiJinShu3_ps=null;
			guiJinShu3_ps=conn.prepareStatement(guiJinShu3);
			guiJinShu3_ps.execute();
			if(guiJinShu3_ps!=null){
				guiJinShu3_ps.close();
			}
			conn.commit();
			//����ծȯ
			//1 ��ծ
			String zhaiQuan1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0301999' WHERE a.guar_no IN  (SELECT a.guaranty_id FROM Grt_P_National_Dbt@CMISTEST2101 a WHERE a.bond_type='1' )";
			System.out.println(zhaiQuan1);
			PreparedStatement zhaiQuan1_ps=null;
			zhaiQuan1_ps=conn.prepareStatement(zhaiQuan1);
			zhaiQuan1_ps.execute();
			if(zhaiQuan1_ps!=null){
				zhaiQuan1_ps.close();
			}
			conn.commit();
			//2 �ط�����ծȯ
			String zhaiQuan2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0302001' WHERE a.guar_no IN  (SELECT a.guaranty_id FROM Grt_P_National_Dbt@CMISTEST2101 a WHERE a.bond_type='2' )";
			System.out.println(zhaiQuan2);
			PreparedStatement zhaiQuan2_ps=null;
			zhaiQuan2_ps=conn.prepareStatement(zhaiQuan2);
			zhaiQuan2_ps.execute();
			if(zhaiQuan2_ps!=null){
				zhaiQuan2_ps.close();
			}
			conn.commit();
			//3 ����ծȯ
			String zhaiQuan3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0303999' WHERE a.guar_no IN  (SELECT a.guaranty_id FROM Grt_P_National_Dbt@CMISTEST2101 a WHERE a.bond_type='3' )";
			System.out.println(zhaiQuan3);
			PreparedStatement zhaiQuan3_ps=null;
			zhaiQuan3_ps=conn.prepareStatement(zhaiQuan3);
			zhaiQuan3_ps.execute();
			if(zhaiQuan3_ps!=null){
				zhaiQuan3_ps.close();
			}
			conn.commit();
			//4 ��ҵծȯ
			String zhaiQuan4="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0304999' WHERE a.guar_no IN  (SELECT a.guaranty_id FROM Grt_P_National_Dbt@CMISTEST2101 a WHERE a.bond_type='4' )";
			System.out.println(zhaiQuan4);
			PreparedStatement zhaiQuan4_ps=null;
			zhaiQuan4_ps=conn.prepareStatement(zhaiQuan4);
			zhaiQuan4_ps.execute();
			if(zhaiQuan4_ps!=null){
				zhaiQuan4_ps.close();
			}
			conn.commit();
			//���� ���õ�������ȫ��ӳ����������
			
			//Ʊ�� 1���гжһ�Ʊ  
			String paioJu1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0401004' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Bill@CMISTEST2101 b WHERE b.bill_type='1' )";
			System.out.println(paioJu1);
			PreparedStatement paioJu1_ps=null;
			paioJu1_ps=conn.prepareStatement(paioJu1);
			paioJu1_ps.execute();
			if(paioJu1_ps!=null){
				paioJu1_ps.close();
			}
			conn.commit();
			//Ʊ�� 2��ҵ�жһ�Ʊ  
			String paioJu2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0401005' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Bill@CMISTEST2101 b WHERE b.bill_type='2' )";
			System.out.println(paioJu2);
			PreparedStatement paioJu2_ps=null;
			paioJu2_ps=conn.prepareStatement(paioJu2);
			paioJu2_ps.execute();
			if(paioJu2_ps!=null){
				paioJu2_ps.close();
			}
			conn.commit();
			//--�ֵ�/�ᵥ 1�ֵ�  
			String cangDan1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY1203001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Depot@CMISTEST2101 b WHERE b.depot_type='10001' )";
			System.out.println(cangDan1);
			PreparedStatement cangDan1_ps=null;
			cangDan1_ps=conn.prepareStatement(cangDan1);
			cangDan1_ps.execute();
			if(cangDan1_ps!=null){
				cangDan1_ps.close();
			}
			conn.commit();
			//--�ֵ�/�ᵥ 2�ᵥ  
			String cangDan2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY1203001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Depot@CMISTEST2101 b WHERE b.depot_type='10001' )";
			System.out.println(cangDan2);
			PreparedStatement cangDan2_ps=null;
			cangDan2_ps=conn.prepareStatement(cangDan2);
			cangDan2_ps.execute();
			if(cangDan2_ps!=null){
				cangDan2_ps.close();
			}
			conn.commit();
			//��Ȩ/��Ʊ/����
			//--1���й�˾��ͨ��
			String guPiao1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0501001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10001' )";
			System.out.println(guPiao1);
			PreparedStatement guPiao1_ps=null;
			guPiao1_ps=conn.prepareStatement(guPiao1);
			guPiao1_ps.execute();
			if(guPiao1_ps!=null){
				guPiao1_ps.close();
			}
			conn.commit();
			//--2���й�˾����ͨ��
			String guPiao2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0501003' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10002' )";
			System.out.println(guPiao2);
			PreparedStatement guPiao2_ps=null;
			guPiao2_ps=conn.prepareStatement(guPiao2);
			guPiao2_ps.execute();
			if(guPiao2_ps!=null){
				guPiao2_ps.close();
			}
			conn.commit();
			//--3�����й�˾��Ȩ
			String guPiao3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0502999' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10003' )";
			System.out.println(guPiao3);
			PreparedStatement guPiao3_ps=null;
			guPiao3_ps=conn.prepareStatement(guPiao3);
			guPiao3_ps.execute();
			if(guPiao3_ps!=null){
				guPiao3_ps.close();
			}
			conn.commit();
			//--4���й�Ȩ
			String guPiao4="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0502001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10004' )";
			System.out.println(guPiao4);
			PreparedStatement guPiao4_ps=null;
			guPiao4_ps=conn.prepareStatement(guPiao4);
			guPiao4_ps.execute();
			if(guPiao4_ps!=null){
				guPiao4_ps.close();
			}
			conn.commit();
			//--5����
			String guPiao5="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0502999' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10005' or b.stock_type is null  )";
			System.out.println(guPiao5);
			PreparedStatement guPiao5_ps=null;
			guPiao5_ps=conn.prepareStatement(guPiao5);
			guPiao5_ps.execute();
			if(guPiao5_ps!=null){
				guPiao5_ps.close();
			}
			conn.commit();
			//--6����
			String guPiao6="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0503001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Stock@CMISTEST2101 b WHERE b.stock_type='10006' )";
			System.out.println(guPiao6);
			PreparedStatement guPiao6_ps=null;
			guPiao6_ps=conn.prepareStatement(guPiao6);
			guPiao6_ps.execute();
			if(guPiao6_ps!=null){
				guPiao6_ps.close();
			}
			conn.commit();
			
			//�浥 1����
			String cunDan1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0101001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Deposit_Rec@CMISTEST2101 b WHERE b.deposit_currency='CNY' )";
			System.out.println(cunDan1);
			PreparedStatement cunDan1_ps=null;
			cunDan1_ps=conn.prepareStatement(cunDan1);
			cunDan1_ps.execute();
			if(cunDan1_ps!=null){
				cunDan1_ps.close();
			}
			conn.commit();
			//�浥 2���
			String cunDan2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0101002' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Deposit_Rec@CMISTEST2101 b WHERE b.deposit_currency!='CNY' )";
			System.out.println(cunDan2);
			PreparedStatement cunDan2_ps=null;
			cunDan2_ps=conn.prepareStatement(cunDan2);
			cunDan2_ps.execute();
			if(cunDan2_ps!=null){
				cunDan2_ps.close();
			}
			conn.commit();
			//Ӧ���˿�1��1���������⴦��
			//����1��1���������⴦��
			//�˻��ʽ���Ѻ1��1���������⴦��
			//��Ѻ������1��1���������⴦��
			
			//**�����಻֪����ζ�Ӧ
			
			//������ת�õ�Ȩ��1��1
			//�����ʲ�1��1 ���������ʲ�
			//��������֤1��1 ��������֤
			//��·�շ�Ȩ1��1 ��·�շ�Ȩ
			//Ӧ�����1��1 Ӧ�����
			//������˰�˻�1��1������˰�˻�
			//ũ������շ�1��1 ũ�������������칤�̵���շ�Ȩ
			//�����շ�Ȩ1��1 �ṩ������������������շ�Ȩ
			
			/**�����Ҫ���ϵͳ�������ֱ����Ǳ���
			String cunDan2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'ZY0101002' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_P_Deposit_Rec@CMISTEST2101 b WHERE b.deposit_currency!='CNY' )";
			PreparedStatement cunDan2_ps=null;
			cunDan2_ps=conn.prepareStatement(cunDan2);
			cunDan2_ps.execute();
			if(cunDan2_ps!=null){
				cunDan2_ps.close();
			}*/
			
			//��Ѻ 
			//����ʹ��Ȩ  1��ҵ�õ�
			String tuDi1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402002' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='1' )";
			System.out.println(tuDi1);
			PreparedStatement tuDi1_ps=null;
			tuDi1_ps=conn.prepareStatement(tuDi1);
			tuDi1_ps.execute();
			if(tuDi1_ps!=null){
				tuDi1_ps.close();
			}
			conn.commit();
			//����ʹ��Ȩ 2��ҵ�õ� 
			String tuDi2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='2' )";
			System.out.println(tuDi2);
			PreparedStatement tuDi2_ps=null;
			tuDi2_ps=conn.prepareStatement(tuDi2);
			tuDi2_ps.execute();
			if(tuDi2_ps!=null){
				tuDi2_ps.close();
			}
			conn.commit();
			//����ʹ��Ȩ 3��ס�õأ�����
			String tuDi3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402001' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='3' )";
			System.out.println(tuDi3);
			PreparedStatement tuDi3_ps=null;
			tuDi3_ps=conn.prepareStatement(tuDi3);
			tuDi3_ps.execute();
			if(tuDi3_ps!=null){
				tuDi3_ps.close();
			}
			conn.commit();
			//����ʹ��Ȩ 4�ۺ��õ�
			String tuDi4="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402004' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='4' )";
			System.out.println(tuDi4);
			PreparedStatement tuDi4_ps=null;
			tuDi4_ps=conn.prepareStatement(tuDi4);
			tuDi4_ps.execute();
			if(tuDi4_ps!=null){
				tuDi4_ps.close();
			}
			conn.commit();
			//����ʹ��Ȩ 5סլ�õ�
			String tuDi5="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402004' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='5' )";
			System.out.println(tuDi5);
			PreparedStatement tuDi5_ps=null;
			tuDi5_ps=conn.prepareStatement(tuDi5);
			tuDi5_ps.execute();
			if(tuDi5_ps!=null){
				tuDi5_ps.close();
			}
			conn.commit();
			//����ʹ��Ȩ 6�����õ�
			String tuDi6="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0402004' WHERE a.guar_no IN  (SELECT b.guaranty_id FROM Grt_G_Land@CMISTEST2101 b WHERE b.land_uses='5' )";
			System.out.println(tuDi6);
			PreparedStatement tuDi6_ps=null;
			tuDi6_ps=conn.prepareStatement(tuDi6);
			tuDi6_ps.execute();
			if(tuDi6_ps!=null){
				tuDi6_ps.close();
			}
			conn.commit();
			
			//�ڽ����̣�ȫ��ӳ�������ڽ�����
			
			
			//���ز� 1���� ��ҵ����
			String fangDiChan1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0203001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type IN ('10001','10002') ) ";
			System.out.println(fangDiChan1);
			PreparedStatement fangDiChan1_ps=null;
			fangDiChan1_ps=conn.prepareStatement(fangDiChan1);
			fangDiChan1_ps.execute();
			if(fangDiChan1_ps!=null){
				fangDiChan1_ps.close();
			}
			conn.commit();
			//���ز� 2��ҵ���� �ֿ�
			String fangDiChan2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0203002' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10003' ) ";
			System.out.println(fangDiChan2);
			PreparedStatement fangDiChan2_ps=null;
			fangDiChan2_ps=conn.prepareStatement(fangDiChan2);
			fangDiChan2_ps.execute();
			if(fangDiChan2_ps!=null){
				fangDiChan2_ps.close();
			}
			conn.commit();
			//���ز� 3��ҵ�÷����̳�
			String fangDiChan3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202003' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10004' ) ";
			System.out.println(fangDiChan3);
			PreparedStatement fangDiChan3_ps=null;
			fangDiChan3_ps=conn.prepareStatement(fangDiChan3);
			fangDiChan3_ps.execute();
			if(fangDiChan3_ps!=null){
				fangDiChan3_ps.close();
			}
			conn.commit();
			//���ز� 4��ҵ�÷�������
			String fangDiChan4="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10005' )";
			System.out.println(fangDiChan4);
			PreparedStatement fangDiChan4_ps=null;
			fangDiChan4_ps=conn.prepareStatement(fangDiChan4);
			fangDiChan4_ps.execute();
			if(fangDiChan4_ps!=null){
				fangDiChan4_ps.close();
			}
			conn.commit();
			//���ز�5��ҵ�÷�������/�Ƶ�
			String fangDiChan5="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202002' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10006' )";
			System.out.println(fangDiChan5);
			PreparedStatement fangDiChan5_ps=null;
			fangDiChan5_ps=conn.prepareStatement(fangDiChan5);
			fangDiChan5_ps.execute();
			if(fangDiChan5_ps!=null){
				fangDiChan5_ps.close();
			}
			conn.commit();
			//���ز�6��ҵ�÷�-�ٽ����з�
			String fangDiChan6="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202999' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10013' )";
			System.out.println(fangDiChan6);
			PreparedStatement fangDiChan6_ps=null;
			fangDiChan6_ps=conn.prepareStatement(fangDiChan6);
			fangDiChan6_ps.execute();
			if(fangDiChan6_ps!=null){
				fangDiChan6_ps.close();
			}
			conn.commit();
			//���ز�7��ҵ�÷�-д�ּ�
			String fangDiChan7="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202999' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10014' )";
			System.out.println(fangDiChan7);
			PreparedStatement fangDiChan7_ps=null;
			fangDiChan7_ps=conn.prepareStatement(fangDiChan7);
			fangDiChan7_ps.execute();
			if(fangDiChan7_ps!=null){
				fangDiChan7_ps.close();
			}
			conn.commit();
			//���ز�8�ۺ��÷�����ס���÷�
			String fangDiChan8="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0202999' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10010' )";
			System.out.println(fangDiChan8);
			PreparedStatement fangDiChan8_ps=null;
			fangDiChan8_ps=conn.prepareStatement(fangDiChan8);
			fangDiChan8_ps.execute();
			if(fangDiChan8_ps!=null){
				fangDiChan8_ps.close();
			}
			conn.commit();
			//���ز�9�ۺ��÷����칫�÷�
			String fangDiChan9="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0201001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10011' )";
			System.out.println(fangDiChan9);
			PreparedStatement fangDiChan9_ps=null;
			fangDiChan9_ps=conn.prepareStatement(fangDiChan9);
			fangDiChan9_ps.execute();
			if(fangDiChan9_ps!=null){
				fangDiChan9_ps.close();
			}
			conn.commit();
			//���ز�10�����÷�����Ԣ��
			String fangDiChan10="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0101002' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10007' )";
			System.out.println(fangDiChan10);
			PreparedStatement fangDiChan10_ps=null;
			fangDiChan10_ps=conn.prepareStatement(fangDiChan10);
			fangDiChan10_ps.execute();
			if(fangDiChan10_ps!=null){
				fangDiChan10_ps.close();
			}
			conn.commit();
			//���ز�11�����÷�������
			String fangDiChan11="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0101003' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10008' )";
			System.out.println(fangDiChan11);
			PreparedStatement fangDiChan11_ps=null;
			fangDiChan11_ps=conn.prepareStatement(fangDiChan11);
			fangDiChan11_ps.execute();
			if(fangDiChan11_ps!=null){
				fangDiChan11_ps.close();
			}
			conn.commit();
			//���ز�12�����÷���סլ
			String fangDiChan12="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0101001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10009' )";
			System.out.println(fangDiChan12);
			PreparedStatement fangDiChan12_ps=null;
			fangDiChan12_ps=conn.prepareStatement(fangDiChan12);
			fangDiChan12_ps.execute();
			if(fangDiChan12_ps!=null){
				fangDiChan12_ps.close();
			}
			conn.commit();
			//���ز�13��������
			String fangDiChan13="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0301001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Real_Estate@CMISTEST2101  a WHERE a.house_type = '10012' )";
			System.out.println(fangDiChan13);
			PreparedStatement fangDiChan13_ps=null;
			fangDiChan13_ps=conn.prepareStatement(fangDiChan13);
			fangDiChan13_ps.execute();
			if(fangDiChan13_ps!=null){
				fangDiChan13_ps.close();
			}
			conn.commit();
			//��ϵͳ��������Ĭ�϶�Ӧ��ϵͳ���ó�
			//�������� 1���̳�
			String jiDongChe1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0701004' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Traffic_Car@CMISTEST2101  a WHERE a.traffic_type IN  ('10001','10003','10004','10008','10009','10010') ) ";
			System.out.println(jiDongChe1);
			PreparedStatement jiDongChe1_ps=null;
			jiDongChe1_ps=conn.prepareStatement(jiDongChe1);
			jiDongChe1_ps.execute();
			if(jiDongChe1_ps!=null){
				jiDongChe1_ps.close();
			}
			conn.commit();
			//�������� 2���ֳ�
			String jiDongChe2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0701003' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Traffic_Car@CMISTEST2101  a WHERE a.traffic_type IN  ('10005') )";
			System.out.println(jiDongChe2);
			PreparedStatement jiDongChe2_ps=null;
			jiDongChe2_ps=conn.prepareStatement(jiDongChe2);
			jiDongChe2_ps.execute();
			if(jiDongChe2_ps!=null){
				jiDongChe2_ps.close();
			}
			conn.commit();
			//�������� 3���ó�
			String jiDongChe3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0701002' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Traffic_Car@CMISTEST2101  a WHERE a.traffic_type IN  ('10006') ) ";
			System.out.println(jiDongChe3);
			PreparedStatement jiDongChe3_ps=null;
			jiDongChe3_ps=conn.prepareStatement(jiDongChe3);
			jiDongChe3_ps.execute();
			if(jiDongChe3_ps!=null){
				jiDongChe3_ps.close();
			}
			conn.commit();
			//�������� 4�γ�
			String jiDongChe4="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0701001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Traffic_Car@CMISTEST2101  a WHERE a.traffic_type IN  ('10007') )";
			System.out.println(jiDongChe4);
			PreparedStatement jiDongChe4_ps=null;
			jiDongChe4_ps=conn.prepareStatement(jiDongChe4);
			jiDongChe4_ps.execute();
			if(jiDongChe4_ps!=null){
				jiDongChe4_ps.close();
			}
			conn.commit();
			//�������� 5����
			String jiDongChe5="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0701999' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Traffic_Car@CMISTEST2101  a WHERE a.traffic_type IN  ('10011','10002') )";
			System.out.println(jiDongChe5);
			PreparedStatement jiDongChe5_ps=null;
			jiDongChe5_ps=conn.prepareStatement(jiDongChe5);
			jiDongChe5_ps.execute();
			if(jiDongChe5_ps!=null){
				jiDongChe5_ps.close();
			}
			conn.commit();
			//��е�豸��Ĭ��ͨ���豸
			//���⴦��רҵ�豸
			String jiXieSheBei1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0602001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Machine@CMISTEST2101  a WHERE a.if_spe_machine = '1' )";
			System.out.println(jiXieSheBei1);
			PreparedStatement jiXieSheBei1_ps=null;
			jiXieSheBei1_ps=conn.prepareStatement(jiXieSheBei1);
			jiXieSheBei1_ps.execute();
			if(jiXieSheBei1_ps!=null){
				jiXieSheBei1_ps.close();
			}
			conn.commit();
			//������Ĭ����������
			//���⴦��1���˴���
			String chuanBo1="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0702001' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Ship@CMISTEST2101  a WHERE a.ship_type = '10001' )";
			System.out.println(chuanBo1);
			PreparedStatement chuanBo1_ps=null;
			chuanBo1_ps=conn.prepareStatement(chuanBo1);
			chuanBo1_ps.execute();
			if(chuanBo1_ps!=null){
				chuanBo1_ps.close();
			}
			conn.commit();
			//���⴦��2����
			String chuanBo2="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0702002' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Ship@CMISTEST2101  a WHERE a.ship_type = '10002' )";
			System.out.println(chuanBo2);
			PreparedStatement chuanBo2_ps=null;
			chuanBo2_ps=conn.prepareStatement(chuanBo2);
			chuanBo2_ps.execute();
			if(chuanBo2_ps!=null){
				chuanBo2_ps.close();
			}
			conn.commit();
			//���⴦��3�洬
			String chuanBo3="UPDATE t_guar_base_info a SET a.guar_type_cd = 'DY0702004' WHERE a.guar_no IN  ( SELECT a.guaranty_id FROM Grt_G_Ship@CMISTEST2101  a WHERE a.ship_type = '10004' )";
			System.out.println(chuanBo3);
			PreparedStatement chuanBo3_ps=null;
			chuanBo3_ps=conn.prepareStatement(chuanBo3);
			chuanBo3_ps.execute();
			if(chuanBo3_ps!=null){
				chuanBo3_ps.close();
			}
			conn.commit();
			//��Ȩ 1��1���������⴦��
			
			//����ʹ��Ȩ��Ӧ������ѺƷ
			
			//��Ѻ�������Ӧ������ѺƷ
			
			//�����Ѻ1��1���������⴦��
			
			//��������Ӧ���������豸���������⴦��
			
			//ũ�����سа���ӪȨ1��1���������⴦��
			
			//����ѺƷ�Ĺؼ��ֶ�1���ؼ��ֶ�2	
			
			//����ѺƷ��������
			PreparedStatement up_low_risk=null;
			String low_risk=" UPDATE t_guar_base_info a SET a.guar_type_cd_cnname=( SELECT b.guar_type_cd_cnname FROM t_guar_class_info b WHERE b.guar_type_cd=a.guar_type_cd ) WHERE a.guar_type_cd IN ( SELECT guar_type_cd FROM t_guar_class_info ) ";
			System.out.println(low_risk);
			up_low_risk=conn.prepareStatement(low_risk);
			up_low_risk.execute();
			conn.commit();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			
			if(up_low_risk!=null){
				up_low_risk.close();
			}
			if(stat!=null){
				stat.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���䵣������Ϣgrt_guaranty_re 2 GRT_GRTCTR_GUAR_REL
	public static void test4(Connection conn) {

		try {			
			
			PreparedStatement query_ps=null;
			String query=" SELECT b.guar_cus_id,b.guar_cus_name,b.guar_cert_typ,b.guar_cert_cde,b.grt_cont_no FROM grt_ctr_cont b  ";
			query_ps=conn.prepareStatement(query);
			ResultSet rs = query_ps.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				count++;
				String cus_id = rs.getString("guar_cus_id")==null?"":rs.getString("guar_cus_id");
				String cus_name = rs.getString("guar_cus_name")==null?"":rs.getString("guar_cus_name");
				String cert_typ = rs.getString("guar_cert_typ")==null?"":rs.getString("guar_cert_typ");;
				String cert_cde = rs.getString("guar_cert_cde")==null?"":rs.getString("guar_cert_cde");;
				String grt_cont_no = rs.getString("grt_cont_no");
				String update="UPDATE GRT_GRTCTR_GUAR_REL a SET a.cus_id='"+cus_id+"',cus_name='"+cus_name+"',a.cert_typ='"+cert_typ+"',a.cert_code='"+cert_cde+"' where a.gurt_cont_no='"+grt_cont_no+"' ";
				pp.addBatch(update);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			if(pp!=null){
				pp.close();
			}
			if(rs!=null){
				rs.close();
			}
			if(query_ps!=null){
				query_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	//���䵣����ͬ�������˵�֤�����͡�֤������
	public static void test5(Connection conn){
		PreparedStatement up_cert_typ_ps=null;
		String cert_typ=" UPDATE grt_ctr_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,cert_code FROM (SELECT b.cus_id, b.cert_type,b.cert_code FROM cus_corp b UNION ALL SELECT c.cus_id, c.cert_type,c.cert_code FROM cus_indiv c) WHERE cus_id=a.cus_id) ";
		System.out.println(cert_typ);
		try {
			up_cert_typ_ps=conn.prepareStatement(cert_typ);
			up_cert_typ_ps.execute();
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

