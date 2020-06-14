package ncmis.data.B_cus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * ������˿ͻ�������Ϣcus_corp,ȱ�٣�����֤����֤�������񱨱�����
 * ������˿ͻ���ͻ������ϵ��cus_loan_rel
 * ע����Ԫ��Ԫ��ת��
 */
public class CusIndiv2CusIndiv {

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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//������˿ͻ�������Ϣ
	/*
	 * SELECT indiv_sps_name,cus_id,input_id FROM cus_indiv@CMISTEST2101 WHERE length(indiv_sps_name)>5,��ż�ͻ�����������Ҫ����
	 */
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("trans_cus_id", "cus_id");//���Ŀͻ���
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250','120','111','130','113','110','114','')");//�ͻ�����
		map.put("indiv_sex", "indiv_sex");//�Ա�
		map.put("cert_type", "decode(cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//֤������
		map.put("cert_code", "cert_code");//֤������
		map.put("indiv_id_beg_dt", "indiv_id_sta_dt");//֤����ʼ�գ�Ϊ�գ�����
		map.put("indiv_id_exp_dt", "indiv_id_exp_dt");//֤����Ч��
		map.put("agri_flg", "decode(agri_flg,'1','Y','2','N','')");//�Ƿ�ũ��
		//**������Ϣ
		map.put("cus_bank_rel", "cus_bank_rel");//�ͻ��Ŵ���ϵ
		map.put("HOLD_STK_AMT", "com_hold_stk_amt");//ӵ�����йɷݽ��
		map.put("bank_duty", "decode(bank_duty,'1','00','2','01','3','02','4','04','5','03')");//������ְ��
		map.put("indiv_ntn", "indiv_ntn");//���壬����ϵͳ�ֵ�����и��£�����Ҫת��
		map.put("country", "indiv_country");//���𣬰���ϵͳ�ֵ�����и��£�����Ҫת��
		map.put("indiv_dt_of_birth", "indiv_dt_of_birth");//��������
		//***������ַ����������
		map.put("brt_province", "''");//������ַ-ʡ
		map.put("brt_city", "''");//������ַ-��
		map.put("brt_area", "''");//������ַ-����
		map.put("brt_town", "''");//������ַ-��/��
		map.put("brt_country", "''");//������ַ-��
		
		map.put("family_income", "family_income");//������ַ,��ϸ��ַ
		map.put("indiv_brt_place", "indiv_houh_reg_add");//������ַ,��ϸ��ַ
		map.put("indiv_pol_st", "decode(indiv_pol_st,'09','05',indiv_pol_st)");//������ò
		map.put("indiv_edt", "decode(indiv_edt,'50','40',indiv_edt)");//���ѧ��,���Ŵ�ȱ�١�50����ѧԺ��
		map.put("indiv_dgr", "decode(indiv_dgr,'9','5','5')");//���ѧλ
		map.put("indiv_heal_st", "indiv_heal_st");//����״��
		map.put("indiv_soc_scr", "decode(indiv_soc_scr,'01','00','02','05','03','01','04','03','05','02','06','04','06')");//��ᱣ�����
		map.put("indiv_hobby", "indiv_hobby");//����
		map.put("indiv_mar_st", "decode(indiv_mar_st,'10','00','20','01','21','02','22','03','23','04','30','05','40','06','90','07')");//����״��
		map.put("indiv_sps_cert_type", "decode(indiv_sps_id_typ,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//��ż֤������
		map.put("indiv_sps_cert_code", "indiv_sps_id_code");//��ż֤������
		map.put("cus_rel_id", "cus_id_rel");//��ż�ͻ���
		map.put("cus_rel_name", "indiv_sps_name");//��ż�ͻ�����
		map.put("indiv_sps_mar_code", "indiv_sps_mar_code");//���֤��(���ڲ���)
		map.put("indiv_sps_occ", "decode(indiv_sps_occ,'0','10000','1','20000','3','30000','4','40000','5','50000','6','60000','7','80100','8','10000','X','70000','Y','80000','Z','80000','80100')");//��żְҵ
		map.put("indiv_scom_name", "indiv_scom_name");//��ż������λ
		map.put("indiv_sps_posl", "decode(indiv_psp_crtfctn,'0','01','1','02','2','03','3','04','9','01')");//ְ��
		map.put("indiv_sps_duty", "decode(indiv_sps_duty,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//ְ��
		map.put("indiv_sps_phone", "indiv_sps_mphn");//��ż�ֻ�����
		map.put("indiv_sps_unit_phe", "indiv_sps_phn");//��ż��λ�绰
		map.put("indiv_sps_annl_income", "indiv_sps_mincm*12");//��ż������(Ԫ)
		map.put("indiv_sps_job_dt", "indiv_sps_job_dt");//��ż�μӹ������
		map.put("rel_dgr", "decode(com_rel_dgr,'01','00','02','01','03','02')");//�����к�����ϵ
		map.put("init_loan_date", "com_init_loan_date");//�����Ŵ���ϵʱ��
		map.put("hold_card", "hold_card");//�ֿ������������ֵ�����滻������ת��
		map.put("passport_flg", "decode(passport_flg,'1','Y','2','N','')");//�Ƿ��й��⻤��
		map.put("crd_grade", "decode(crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//���õȼ�
		map.put("crd_date", "crd_date");//������������
		map.put("remark", "remark");//��ע
//		map.put("cus_ccr_model_id", "");//����ģ�ͣ���ϵͳ�޴��ֶΣ�����ֲ
//		map.put("cus_ccr_model_name", "");//����ģ�����ƣ���ϵͳ�޴��ֶΣ�����ֲ	
		
		/*
		 * ���˿ͻ�-��ϵ��Ϣ��ǩ
		 */
		//**���⴦��
		/*
		map.put("post_province", "");//����ͨѶ��ַ
		map.put("post_city", "");//����ͨѶ��ַ		
		map.put("post_town", "");//����ͨѶ��ַ
		map.put("post_country", "");//����ͨѶ��ַ
		*/
		map.put("post_area", "area_code");//����ͨѶ��ַ
		map.put("post_addr", "post_addr");//ͨѶ��ַ-��ϸ��ַ
		map.put("post_code", "post_code");//ͨѶ��ַ-��������
		
		//**
		/*
		map.put("rsd_province", "");//������ס��ַ
		map.put("rsd_city", "");//������ס��ַ
		map.put("rsd_area", "");//������ס��ַ
		map.put("rsd_town", "");//������ס��ַ
		map.put("rsd_country", "");//������ס��ַ
		*/
		map.put("indiv_rsd_addr", "indiv_rsd_addr");//��ס��ַ-��ϸ��ַ
		map.put("indiv_rsd_st", "decode(indiv_rsd_st,'1','10','2','20','3','30','4','40','5','50','6','60','99')");//��ס��ַ-��ס״��
		map.put("indiv_zip_code", "indiv_zip_code");//��ס��ַ-��ס״��
		map.put("fphone", "fphone");//סլ�绰
		map.put("mobile", "mobile");//�ֻ�����
		map.put("phone", "phone");//����֪ͨ����
		map.put("fax_code", "fax_code");//����
		map.put("email", "email");//����
		map.put("indiv_occ", "decode(indiv_occ,'0','10000','1','20000','3','30000','4','40000','5','50000','6','60000','7','80100','8','10000','X','70000','Y','80000','Z','80100','80100')");//ְҵ
		map.put("email", "email");//����
		map.put("unit_name", "indiv_com_name");//������λ
		//**
		map.put("unit_typ", "decode(indiv_com_typ,'100','3400','200','2000','300','9000','400','4000','510','1110','520','1120','530','1160','540','1130','550','1140','560','1150','570','1170','600','1290','610','1310','620','1320','630','1330','640','1340','700','1175','9000')");//��λ����500 δ��
		
		map.put("unit_fld", "indiv_com_fld");//��λ������ҵ����
		map.put("unit_fld_name", "indiv_com_fld_name");//��λ������ҵ����
		/*
		 * ��λ������ַ
		map.put("unit_province", "");//��λ��ַ
		map.put("unit_city", "");//��λ��ַ
		map.put("unit_area", "");//��λ��ַ
		map.put("unit_town", "");//��λ��ַ
		map.put("unit_country", "");//��λ��ַ
		*/
		
		map.put("unit_zip_code", "indiv_com_zip_code");//��λ�ʱ�
		map.put("unit_phn", "indiv_com_phn");//��λ�绰
		map.put("unit_fax", "indiv_com_fax");//��λ����
		map.put("unit_cnt_name", "indiv_com_cnt_name");//��λ��ϵ��
		map.put("indiv_work_job_y", "indiv_work_job_y");//��λ������ʼ��
		map.put("job_ttl", "decode(indiv_com_job_ttl,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//ְ��
		map.put("indiv_crtfctn", "indiv_crtfctn");//ְ��
		map.put("indiv_ann_incm", "indiv_ann_incm*12");//������(Ԫ)
		//**
//		map.put("indiv_ann_incm_src", "");//������Դ
		map.put("indiv_sal_acc_bank", "indiv_sal_acc_bank");//�����˻�������
		map.put("indiv_sal_acc_no", "indiv_sal_acc_no");//�����˺�
		map.put("work_resume", "work_resume");//���˼���
		map.put("cus_status", "decode(cus_status,'00','00','20','20','01','01','02','00','03','20')");//�ͻ�״̬
		//**
//		map.put("main_br_id", "SELECT a.usr_bch FROM s_usr a WHERE a.usr_cde=''");//���ܻ���
		
		map.put("cust_mgr", "decode(cust_mgr,'',input_id,cust_mgr)");//cust_mgr���ܿͻ�����decode(cust_mgr,'',input_id,cust_mgr)
		map.put("input_id", "input_id");//�Ǽ���
		map.put("input_br_id", "input_br_id");//�Ǽǻ���
		map.put("input_date", "input_date");//�Ǽ�����
		map.put("last_chg_usr", "last_upd_id");//������
		map.put("last_chg_dt", "last_upd_date");//��������
		map.put("instu_cde", "'0000'");//���˱���
		
		map.put("shahd_ind", "decode(is_bank_stk,'1','Y','2','N','')");//�Ƿ�ɶ�
		
		//**����
		map.put("shares_org", "''");//��ɻ���������
		map.put("shares_dt", "''");//���ʱ�䣬����
		map.put("unit_fld2", "''");//������ҵ2������
		map.put("unit_fld_name2", "''");//������ҵ2���ƣ�����
		map.put("faml_regs_no", "''");//���ڲ��ţ�����
		map.put("indiv_oth_alle_ind", "''");//����������ƶ��־������
		map.put("alle_bunb", "''");//��ƶ��������������
		map.put("large_agri_iind", "''");//ũҵרҵ�󻧱�־������
		map.put("fami_farm_ind", "''");//��ͥũ����־������
		map.put("idval_type", "''");//���徭Ӫ�����ͣ�����
		map.put("idval_type", "''");//���徭Ӫ�����ͣ�����
		map.put("inclusive_finance_statistics", "''");//�ջݽ���ͳ�ƿھ�������
		map.put("is_low_basic", "''");//�Ƿ�ͱ���������
		map.put("is_low_basic", "''");//�Ƿ�ͱ���������
		
		
		
		map.put("shares_amt", "com_hold_stk_amt");//��ɽ�Ԫ��
		map.put("undertaking_guarant", "under_guar_cus_type");//��ҵ������12����
		
		//**
		map.put("nat_eco_sec", "''");//���񾭼ò���
		map.put("unit_ind", "decode(indiv_scom_name,'','N','Y')");//�Ƿ��й�����λ
		
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
			String de_cus_corp=" truncate table cus_indiv ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_indiv ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM cus_indiv@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			//�������ܿͻ��������ڵ�����
			PreparedStatement up_cust_mgr_ps=null;
			String cust_mgr_id=" UPDATE cus_indiv a SET a.cust_mgr = a.input_id WHERE a.cust_mgr NOT IN ( SELECT b.usr_cde FROM s_usr b  )";
			up_cust_mgr_ps=conn.prepareStatement(cust_mgr_id);
			up_cust_mgr_ps.execute();
			if(up_cust_mgr_ps!=null){
				up_cust_mgr_ps.close();
			}
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE cus_indiv a SET a.main_br_id= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.cust_mgr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
						
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE cus_indiv a SET input_br_id= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.input_id) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			//������������-��
			PreparedStatement up_reg_city_ps=null;
			String reg_city_ps=" UPDATE cus_indiv a SET a.post_city=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.post_area) ";
			up_reg_city_ps=conn.prepareStatement(reg_city_ps);
			up_reg_city_ps.execute();
			if(up_reg_city_ps!=null){
				up_reg_city_ps.close();
			}
			
			//������������-ʡ
			PreparedStatement up_reg_province_ps=null;
			String reg_province_ps=" UPDATE cus_indiv a SET a.post_province=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.post_city) ";
			up_reg_province_ps=conn.prepareStatement(reg_province_ps);
			up_reg_province_ps.execute();
			if(up_reg_province_ps!=null){
				up_reg_province_ps.close();
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
	
	//������˿ͻ���ϵ��cus_loan_rel 
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("PK_ID", "cus_id");//�ͻ���
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250','120','111','130','113','110','114','')");//�ͻ�����
//		map.put("cus_short_name", "cus_short_name");//�ͻ����
//		map.put("cus_en_name", "cus_en_name");//��������
		map.put("cert_type", "decode(cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//֤������
		map.put("cert_code", "cert_code");//֤������
		map.put("CUS_MGR_TYPE", "01");//
		map.put("CUS_MGR", "decode(cust_mgr,'',input_id,cust_mgr)");//
//		map.put("BR_ID", "main_br_id");//
//		map.put("AGRI_FLG", "main_br_id");//
		
//		map.put("MAIN_ORG_FLG", "cus_short_name");//
//		map.put("CUS_OP_TYPE", "cus_en_name");//
		map.put("TRANS_CUS_ID", "cus_id");//
		map.put("CUS_STATUS", "decode(cus_status,'00','00','20','20','01','01','02','00','03','20')");//
//		map.put("SERNO", "01");//
//		map.put("IDVAL_TYPE", "");//
//		map.put("SOCIAL_CREDIT_CODE_IND", "main_br_id");//
//		map.put("SOCI_CRED_CODE", "main_br_id");//
		
		
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
			String de_cus_corp=" DELETE FROM cus_loan_rel a WHERE a.cus_id IN ( SELECT cus_id FROM cus_indiv ) ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_loan_rel ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM cus_indiv@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE cus_loan_rel a SET BR_ID= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.cus_mgr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

}
