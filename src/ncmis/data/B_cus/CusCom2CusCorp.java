package ncmis.data.B_cus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * ����Թ��ͻ�������Ϣcus_corp,ȱ�٣�����֤����֤�������񱨱�����
 * ����Թ��ͻ���ͻ������ϵ��cus_loan_rel
 * ����Թ��ͻ��߹���Ϣ cus_corp_mgr,�߹ܵ�������Ҫ�޸ģ���cus_id��CUS_REL_ID��Ϊcus_id��CUS_REL_ID��MRG_TYPE
 * ����Թ��ͻ��ʱ�������Ϣ
 */
public class CusCom2CusCorp {

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
			test6(conn);
			test7(conn);
			test8(conn);
			test9(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//����Թ��ͻ�������Ϣ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("trans_cus_id", "cus_id");//�ͻ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");//�ͻ�����
		map.put("cert_type", "decode(cert_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','29900')");//֤������
		map.put("cert_code", "cert_code");//֤������
		map.put("cus_status", "decode(cus_status,'00','00','20','20','01','01','02','00','03','20')");//�ͻ�״̬
		
		map.put("cus_short_name", "cus_short_name");//�ͻ����
		map.put("cus_en_name", "cus_en_name");//��������
		map.put("country", "com_country");//����
		map.put("com_lcl_flg", "decode(com_lcl_flg,'1','Y','2','N','')");//�Ƿ����
		map.put("city_flg", "decode(com_city_flg,'10','00','11','01','20','02','21','03','')");//��������
		map.put("invest_type", "invest_type");//Ͷ������,�ֵ�һ�£�����ӳ��
		map.put("sub_typ", "decode(com_sub_typ,'1','01','2','04','3','04','4','04','5','04','7','02','8','03','6','04')");//������ϵ ���
		map.put("hold_type", "decode(com_hold_type,'111','010','112','020','121','030','122','040','171','050','172','060','201','070','202','080','301','090','302','100','110')");//���
		map.put("cll_type", "com_cll_type");//��ҵ����
		map.put("cll_name", "com_cll_name");//��ҵ��������
		map.put("in_reg_state_code", "in_reg_state_code");//��ҵ�������
		map.put("in_cll_type", "in_cll_type");//��ҵ��ҵ����
		map.put("str_date", "com_str_date");//��������
		map.put("employee", "com_employee");//��ҵ����
		map.put("scale_cll_type", "decode(com_sta_type,'1','00','2','01','3','02','4','03','5','04','6','05','7','09','8','06','9','07','10','08','11','11','12','12','13','10','14','13','15','14','16','15','')");//��ҵ��ģ��ҵ���ͣ�����ᣩ
		map.put("sal_volume", "com_sales_amt/10000");//���۶��Ԫ��������10000
		map.put("oper_income", "com_sales_amt/10000");//Ӫҵ���루��Ԫ������10000
		map.put("ass_total", "com_sum_amt/10000");//�ʲ��ܶ��Ԫ��������10000
		map.put("cus_scale", "com_scale");//��ҵ��ģ
		map.put("is_trade_enterprises", "decode(is_trade_enterprises,'1','Y','N')");//�Ƿ�ó����ҵ
		map.put("is_green_market", "decode(is_green_market,'1','Y','N')");//�Ƿ���ɫ�Ŵ���ҵ
		map.put("technology_flag", "technology_flag");//�Ƽ��ƴ���ҵ��ʶ
//		map.put("nat_eco_sec", "technology_flag");//���񾭼ò��ű��
//		map.put("unit_alle_ind", "");//��ҵ��ƶ��λ��
//		map.put("alle_bunb", "");//��ƶ��������		
//		map.put("fram_ind", "");//ũ����־	
//		map.put("farmer_cop_ind", "");//ũ��רҵ�������־		
//		map.put("agr_ind", "");//�Ƿ�ũ�弯�徭����֯	
		map.put("ins_code", "com_ins_code");//��֯��������
		map.put("ins_reg_dt", "com_ins_reg_date");//��֯�����Ǽ�����
		map.put("ins_reg_end_dt", "com_ins_exp_date");//��֯�����Ǽ���Ч��
		map.put("ins_org", "com_ins_org");//��֯��������֤�䷢����
		map.put("ins_ann_date", "com_ins_ann_date");//��֯��������֤��쵽������
//		map.put("license_type", "reg_type");//ע��ǼǺ�����
		map.put("reg_code", "reg_code");//ע��ǼǺ�
		map.put("reg_type", "decode(reg_type,'110','1110','120','1120','130','1130','141','1131','142','1132','143','1133','149','1139','151','1140','159','1149','160','1150','171','1171','172','1172','173','1173','174','1174','175','1300','176','1200','210','1210','220','1220','230','1230','240','1240','310','1310','320','1320','330','1330','340','1340','410','1175','420','1175','510','9000','520','4000','530','9000','540','9000','550','9000','560','2000','570','3400','580','9000')");//ע��Ǽ�����
		map.put("admin_org", "admin_org");//���ܵ�λ
		map.put("appr_org", "appr_org");//��׼����
		map.put("appr_doc_no", "appr_doc_no");//��׼�ĺ�
//		map.put("reg_province", "");//ע��Ǽǵ�ַ-ʡ
//		map.put("reg_city", "");//ע��Ǽǵ�ַ-��
		map.put("reg_area", "reg_state_code");//ע��Ǽǵ�ַ-��/��
//		map.put("reg_town", "");//ע��Ǽǵ�ַ-��/��
//		map.put("reg_country", "");//ע��Ǽǵ�ַ-��
		map.put("reg_addr", "reg_addr");//��ϸ��ַ
		map.put("en_reg_addr", "en_reg_addr");//����ע��Ǽǵ�ַ
//		map.put("acu_province", "");//ʵ�ʾ�Ӫ��ַ-ʡ
//		map.put("acu_city", "");//ʵ�ʾ�Ӫ��ַ-��
//		map.put("acu_area", "");//ʵ�ʾ�Ӫ��ַ-��/��
//		map.put("acu_town", "");//ʵ�ʾ�Ӫ��ַ-��/��
//		map.put("acu_country", "");//ʵ�ʾ�Ӫ��ַ-��
		map.put("acu_addr", "acu_addr");//ʵ�ʾ�Ӫ��ַ-��ϸ��ַ
		map.put("main_opt_scp", "com_main_opt_scp");//��Ӫҵ��Χ
		map.put("part_opt_scp", "com_part_opt_scp");//��Ӫҵ��Χ
		map.put("reg_cur_type", "reg_cur_type");//ע���ʱ�/�����ʽ����
		map.put("reg_cap_amt", "reg_cap_amt/10000");//ע���ʱ�����Ԫ��������10000
		map.put("paid_cap_cur_type", "reg_cur_type");//ʵ���ʱ����֣�����ϵͳע�����һ��
		map.put("paid_cap_amt", "paid_cap_amt/10000");//ʵ���ʱ�(��Ԫ)������10000
		map.put("reg_dt", "reg_start_date");//ע��Ǽ�����
		map.put("reg_end_dt", "reg_end_date");//ע��Ǽ���Ч��
		map.put("ann_date", "reg_audit_end_date");//��쵽����
//		map.put("reg_area_code2", "reg_audit_end_date");//ע����������
//		map.put("reg_audit_date", "");//ע��Ǽ���������
		map.put("loc_tax_reg_code", "loc_tax_reg_code");//��˰˰��ǼǴ���
		map.put("loc_tax_reg_dt", "loc_tax_reg_dt");//��˰˰��Ǽ�����
		map.put("loc_tax_reg_end_dt", "loc_tax_reg_end_dt");//��˰˰��Ǽ���Ч��
		map.put("loc_tax_ann_date", "reg_audit_end_date");//��˰��쵽����
		map.put("loc_tax_reg_org", "loc_tax_reg_org");//��˰�Ǽǻ���
		map.put("nat_tax_reg_code", "nat_tax_reg_code");//��˰˰��ǼǴ���
		map.put("nat_tax_reg_dt", "nat_tax_reg_dt");//��˰˰��Ǽ�����
		map.put("nat_tax_reg_end_dt", "nat_tax_reg_end_dt");//��˰˰��Ǽ���Ч��
		map.put("nat_tax_ann_date", "reg_audit_end_date");//��˰��쵽����
		map.put("nat_tax_reg_org", "nat_tax_reg_org");//��˰�Ǽǻ���
		map.put("com_sp_business", "decode(com_sp_business,'1','Y','N')");//���־�Ӫ��ʶ
		map.put("com_sp_lic_no", "com_sp_lic_no");//���־�Ӫ��ʶ
		map.put("com_sp_detail", "com_sp_detail");//���־�Ӫ���
		map.put("com_sp_lic_org", "com_sp_lic_org");//�������֤�䷢����
		map.put("com_sp_str_date", "com_sp_str_date");//���־�Ӫ�Ǽ�����
		map.put("com_sp_end_date", "com_sp_end_date");//���־�Ӫ��������
		
		//�����Ϣ
		map.put("loan_card_flg", "decode(loan_card_flg,'1','1','2','0','')");//���޴��
		map.put("loan_card_id", "loan_card_id");//�����
		map.put("loan_card_pwd", "loan_card_pwd");//�������
		map.put("loan_card_eff_flg", "decode(loan_card_eff_flg,'01','1','03','2','3')");//���״̬
		map.put("loan_card_end_date", "loan_card_audit_dt");//�����쵽����
		
		map.put("taxpayer_code", "decode(cert_type,'29',cert_code,'')");//��˰��ʶ���
//		map.put("deft_flag", "");//ΥԼ��ʾ
//		map.put("deft_date", "");//ΥԼ����
//		map.put("deft_days", "");//ΥԼ����
		
		//�����˻���Ϣ
		map.put("bas_acc_flg", "decode(bas_acc_flg,'1','Y','2','N','')");//��������˻��Ƿ��ڱ�����
		map.put("bas_acc_licence", "bas_acc_licence");//��������˻��������֤
		map.put("bas_acc_bank", "bas_acc_bank");//��������˻�������
		map.put("bas_acc_no", "bas_acc_no");//��������˻��˺�
		map.put("bas_acc_date", "bas_acc_date");//�����˻���������
		map.put("bas_acc_date", "bas_acc_date");//�����˻���������
		
		//���������˼�����ż��Ϣ
		map.put("legal_cert_type", "decode(legal_cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//����������֤������
		map.put("legal_cert_code", "legal_cert_code");//����������֤������
		map.put("reg_org_legal", "reg_org_legal");//����������֤������
		map.put("legal_no", "cus_id_rel");//���������˿ͻ����
		map.put("legal_name", "legal_name");//��������������
		map.put("legal_sex", "''");//�����������Ա�,��ϵͳû��
		map.put("legal_resume", "cus_work_resume");//���������˸��˼���
		map.put("legal_sign_init_date", "legal_sign_init_date");//����������ǩ��������ʼ����
		map.put("legal_sign_end_date", "legal_end_init_date");//����������ǩ��������������
		map.put("legal_phone", "mobile");//�����������ֻ���
//		map.put("legal_mar_st", "");//���������˻������
		map.put("legal_sps_cert_type", "decode(legal_sps_id_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//������������ż֤������
		map.put("legal_sps_cert_code", "legal_sps_id_code");//������������ż֤������
		map.put("legal_sps_no", "legal_sps_no");//������������ż�ͻ�����
		map.put("legal_sps_name", "legal_sps_name");//������������ż����
//		map.put("legal_sps_phone", "");//������������ż�ֻ���
		
		//ʵ�ʿ�������Ϣ
		map.put("real_conl_cert_type", "decode(reality_controler_id_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100','')");//ʵ�ʿ�����֤������,��Ҫת��
		map.put("real_conl_cert_code", "reality_controler_id_code");//ʵ�ʿ�����֤������
		map.put("real_conl_no", "reality_controler_no");//ʵ�ʿ����˿ͻ���
		map.put("real_conl_name", "reality_controler_name");//ʵ�ʿ���������
		map.put("real_sign_init_date", "real_sign_init_date");//ʵ�ʿ�����ǩ��������ʼ����
		map.put("real_sign_end_date", "real_sign_end_date");//ʵ�ʿ�����ǩ��������������
		map.put("real_accredit_init_date", "real_accredit_init_date");//ʵ�ʿ�������Ȩ�鿪ʼ����
		map.put("real_accredit_end_date", "real_accredit_end_date");//ʵ�ʿ�������Ȩ���������
		map.put("real_work_resume", "real_work_resume");//ʵ�ʿ����˸��˼���
//		map.put("real_conl_phone", "legal_sps_name");//ʵ�ʿ������ֻ���
//		map.put("real_mar_st", "legal_sps_name");//ʵ�ʿ����˻������
		map.put("real_sps_cert_type", "decode(reality_sps_id_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100','')");//ʵ�ʿ�������ż֤������
		map.put("real_sps_cert_code", "reality_id_code");//ʵ�ʿ�������ż֤������
		map.put("real_sps_no", "reality_sps_no");//ʵ�ʿ�������ż�ͻ����
		map.put("real_sps_name", "reality_sps_name");//ʵ�ʿ�������ż����
//		map.put("real_sps_phone", "legal_sps_name");//ʵ�ʿ�������ż�ֻ���
		
		//������Ϣ
		map.put("fna_mgr", "fna_mgr");//���񲿸�����
//		map.put("fna_telephone", "reality_controler_id_code");//���񲿹̶��绰
		map.put("fna_contact", "com_operator");//������ϵ��
//		map.put("fna_con_phone", "reality_controler_name");//������ϵ�˹̻�
		map.put("fna_con_mobile_phone", "phone");//������ϵ���ֻ���
		
		//��ַ��Ϣ
		map.put("post_addr", "post_addr");//��ϸ��ַ
		map.put("phone", "legal_phone");//�ֻ�����
//		map.put("landlin_telephone", "");//�̶��绰
		map.put("fax_code", "fax_code");//����
		map.put("email", "email");//Email
		map.put("web_url", "web_url");//��ַ
		map.put("post_code", "post_code");//��������
		
		//�ſ���Ϣ
		map.put("mrk_flg", "decode(com_mrk_flg,'1','Y','2','N','')");//�Ƿ�����
		map.put("mrk_area", "com_mrk_area");//���е�
		map.put("stock_id", "com_stock_id");//��Ʊ����
		map.put("is_gov_plat", "decode(is_government,'1','Y','2','N','')");//�Ƿ���������ƽ̨is_government
		map.put("imp_exp_flg", "decode(com_imp_exp_flg,'1','Y','2','N','')");//�Ƿ�ӵ�н�����Ȩ
		map.put("fexc_prm_code", "com_fexc_prm_code");//������֤��
		map.put("main_product", "com_main_product");//��Ҫ�������
		map.put("prod_equip", "com_prod_equip");//��Ҫ�����豸
		map.put("fact_prod", "com_fact_prod");//��Ҫ��������
		map.put("opt_aera", "com_opt_aera");//��Ӫ�������(ƽ����)
		map.put("opt_owner", "decode(com_opt_owner,'1','01','2','02','3','99')");//��Ӫ��������Ȩ����ֵһ��
		map.put("opt_st", "decode(com_opt_st,'100','000','200','001','002')");//��Ӫ״��
		map.put("imptt_flg", "decode(com_imptt_flg,'1','Y','2','N','')");//�����ص���ҵ
		map.put("prep_flg", "decode(com_prep_flg,'1','Y','2','N','')");//������ҵ
//		map.put("is_hi_tech", "");//������ҵ
		map.put("dhgh_flg", "decode(com_dhgh_flg,'1','Y','2','N','')");//�߻�����Ⱦ������ҵ
//		map.put("ind_stru_adj_type", "reality_controler_id_type");//��ҵ�ṹ��������
		map.put("ci_entp", "decode(com_cl_entp,'1','Y','2','N','')");//���Һ�۵����޿ز�ҵ
//		map.put("ind_trans_flag", "");//��ҵת��������־
//		map.put("stra_ind_type", "reality_controler_name");//ս�����˲�ҵ����
		map.put("hd_enterprise", "decode(com_hd_enterprise,'1','Y','2','N','')");//��ͷ��ҵ
//		map.put("is_new_corp", "");//�Ƿ��½���ҵ
//		map.put("main_scp_country", "");//��Ҫҵ�����ڹ���
//		map.put("mng_org", "");//�ϼ����ܵ�λ
		
//		map.put("fin_rep_type", "");//���񱨱����ʹ�����
		
		//������Ϣ
		map.put("cus_bank_rel", "decode(cus_bank_rel,'B1','B1','A2','A2','C6','A2','C7','A2','C8','A2','C9','A2','D2','A2','D3','A2','D8','A2','D9','A2','B4','A3','B6','A3','A5','A5','')");//�ͻ��Ŵ���ϵ
//		map.put("rel_stat_adj_date", "com_mrk_area");//��ϵ״̬����ʱ��
//		map.put("rel_stat_adj_reason", "com_stock_id");//��ϵ����ԭ��
		map.put("init_loan_date", "com_init_loan_date");//�����Ŵ���ϵʱ��
		map.put("crd_grade", "decode(com_crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//���õȼ�(�ڲ�)
		map.put("crd_dt", "com_crd_dt");//������������
		map.put("out_crd_grade", "decode(com_out_crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//���õȼ�(�ⲿ)
		map.put("out_crd_dt", "com_out_crd_dt");//������������(�ⲿ)
		map.put("out_crd_org", "com_out_crd_org");//��������(�ⲿ)
		
		//�ܻ���
		map.put("cust_mgr", "decode(cust_mgr,'',input_id,cust_mgr)");
//		map.put("main_br_id", "SELECT a.usr_bch FROM s_usr a WHERE a.usr_cde=''");//���ܻ���
		map.put("input_id", "input_id");//�Ǽ���
		map.put("input_br_id", "input_br_id");//�Ǽǻ���
		map.put("input_date", "input_date");//�Ǽ�����
		map.put("last_chg_usr", "last_upd_id");//������
		map.put("last_chg_dt", "last_upd_date");//��������
		map.put("instu_cde", "'0000'");//���˱���
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
/*
 * map.put("fin_rep_type", "");//���񱨱����ʹ�����
 */
		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table  cus_corp ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_corp ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM cus_com@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			//���·����������Ա𣬸��ݷ������������֤���жϣ������ڶ�λ������Ϊ�У�˫��ΪŮ
			PreparedStatement up_legal_sex_ps=null;
			String legal_sex_ps=" UPDATE cus_corp a SET a.legal_sex = ( SELECT decode(mod(substr(c.legal_cert_code,17,1),2),0,'2',1,'1','3') FROM cus_corp c WHERE c.cus_id=a.cus_id) WHERE a.legal_cert_type='10100' AND length(legal_cert_code)=18 ";
			up_legal_sex_ps=conn.prepareStatement(legal_sex_ps);
			up_legal_sex_ps.execute();
			if(up_legal_sex_ps!=null){
				up_legal_sex_ps.close();
			}
			//������������-��
			PreparedStatement up_reg_city_ps=null;
			String reg_city_ps=" UPDATE cus_corp a SET a.reg_city=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.reg_area) ";
			up_reg_city_ps=conn.prepareStatement(reg_city_ps);
			up_reg_city_ps.execute();
			if(up_reg_city_ps!=null){
				up_reg_city_ps.close();
			}
			
			//������������-ʡ
			PreparedStatement up_reg_province_ps=null;
			String reg_province_ps=" UPDATE cus_corp a SET a.reg_province=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.reg_city) ";
			up_reg_province_ps=conn.prepareStatement(reg_province_ps);
			up_reg_province_ps.execute();
			if(up_reg_province_ps!=null){
				up_reg_province_ps.close();
			}
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE cus_corp a SET a.main_br_id= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.cust_mgr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
						
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE cus_corp a SET input_br_id= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.input_id) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			PreparedStatement up_fnc_type_1_ps=null;
			String up_fnc_type_1=" UPDATE cus_corp a SET a.fnc_type='PB0001' WHERE a.cus_type IN ('211','251') ";
			up_fnc_type_1_ps=conn.prepareStatement(up_fnc_type_1);
			up_fnc_type_1_ps.execute();
			if(up_fnc_type_1_ps!=null){
				up_fnc_type_1_ps.close();
			}
			//������Ϊ�յ�����Ĭ��Ϊ�й�
			PreparedStatement up_country_ps=null;
			String up_country_1=" UPDATE cus_corp a SET country='CHN' WHERE a.country is null ";
			up_country_ps=conn.prepareStatement(up_country_1);
			up_country_ps.execute();
			if(up_country_ps!=null){
				up_country_ps.close();
			}
			
			PreparedStatement up_fnc_type_2_ps=null;
			String up_fnc_type_2=" UPDATE cus_corp a SET a.fnc_type='PB0003' WHERE a.cus_type NOT IN ('211','251') ";
			up_fnc_type_2_ps=conn.prepareStatement(up_fnc_type_2);
			up_fnc_type_2_ps.execute();
			if(up_fnc_type_2_ps!=null){
				up_fnc_type_2_ps.close();
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
	
	//����Թ��ͻ���ϵ��cus_loan_rel 
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("PK_ID", "cus_id");//�ͻ���
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");//�ͻ�����
		map.put("cus_short_name", "cus_short_name");//�ͻ����
		map.put("cus_en_name", "cus_en_name");//��������
		map.put("cert_type", "decode(cert_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//֤������
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
			String de_cus_corp=" DELETE FROM cus_loan_rel a WHERE a.cus_id IN ( SELECT cus_id FROM cus_com@CMISTEST2101 )  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_loan_rel ("+keys.toString().substring(1)+") SELECT "+values.toString().substring(1)+" FROM cus_com@CMISTEST2101 b ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE cus_loan_rel a SET BR_ID= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.cus_mgr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
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

	
	/*
	 * ����Թ��ͻ��߹���Ϣ��cus_corp_mgr 
	 * 1 ��ϵͳ�߹ܿͻ���Ϊ�գ�����
	 * 2 ��ϵͳ�߹������������ڶ��һ������������ͻ������һһ��Ӧ��'02','03','04','05','10'������������
	 */
	public static void test3(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("cus_rel_id", "cus_id_rel");//�ͻ���
		map.put("mrg_type", "decode(com_mrg_typ,'02','05','03','02','04','03','05','06','10','07','08')");//�߹�����'02','03','04','05','10'
		map.put("mrg_cert_type", "decode(com_mrg_cert_typ,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100')");//�߹�֤������
		map.put("mrg_cert_code", "com_mrg_cert_code");//�߹�֤������
		map.put("mrg_name", "com_mrg_name");//�߹�����
		map.put("mrg_sex", "com_mrg_sex");//�Ա�
		map.put("mrg_bday", "com_mrg_bday");//��������
		map.put("mrg_occ", "decode(com_mrg_occ,'0','01','1','02','3','03','4','04','5','05','6','06','8','01','X','07','Y','08','Z','09','09')");//ְҵ
		map.put("MRG_DUTY", "decode(com_mrg_duty,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//ְ��
		map.put("MRG_CRTF", "decode(com_mrg_crtf,'0','01','1','02','2','03','3','04','9','01')");//ְ��
		map.put("MRG_EDT", "com_mrg_edt");//���ѧ��,��ֵһ��
		map.put("MRG_DGR", "decode(com_mrg_dgr,'1','0','2','1','3','2','4','3','9','5','0','4')");//���ѧλ
		map.put("SIGN_INIT_DATE", "sign_init_date");//ǩ��������ʼ����
		map.put("SIGN_END_DATE", "sign_end_date");//ǩ��������������
		map.put("ACCREDIT_INIT_DATE", "accredit_init_date");//��Ȩ�鿪ʼ����
		map.put("ACCREDIT_END_DATE", "accredit_end_date");//��Ȩ�鵽������
		map.put("RESUME", "remark");//��������
		map.put("INPUT_ID", "input_id");//�Ǽ���
		map.put("INPUT_BR_ID", "input_br_id");//�Ǽǻ���
		map.put("INPUT_DATE", "input_date");//�Ǽ�����
		map.put("LAST_CHG_USR", "last_upd_id");//��������
		map.put("LAST_CHG_DT", "last_upd_date");//����������
		map.put("INSTU_CDE", "'0000'");//���˱���
//		map.put("REMARK", "");//
		map.put("MRG_PHONE", "com_mrg_mphn1");//��ϵ�绰
		
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
			String de_cus_corp=" truncate table cus_corp_mgr  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_corp_mgr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM cus_com_manager@CMISTEST2101 b WHERE b.cus_id_rel is not null and b.com_mrg_typ in ('02','03','04','05','10')) ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
//			PreparedStatement r_insert_cus_corp_ps=null;
//			String r_insert_cus_corp=" INSERT INTO cus_corp_mgr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM cus_com_manager@CMISTEST2101 b WHERE b.cus_id_rel is not null and b.com_mrg_typ in ('02','03','04','05','10')) ";
//			System.out.println(r_insert_cus_corp);
//			
//			r_insert_cus_corp_ps=conn.prepareStatement(r_insert_cus_corp);
//			r_insert_cus_corp_ps.execute();
//			if(r_insert_cus_corp_ps!=null){
//				r_insert_cus_corp_ps.close();
//			}
			
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
	
	/*
	 * ����Թ��ͻ��ʱ�������Ϣ��cus_corp_apital 
	 * 1 ��ϵͳ�߹ܿͻ���Ϊ�գ�����
	 * 2 ��ϵͳ�߹������������ڶ��һ������������ͻ������һһ��Ӧ��'02','03','04','05','10'������������
	 */
	public static void test4(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("cus_rel_id", "cus_id_rel");//�ͻ���
		map.put("invt_typ", "decode(invt_typ,'1','1','2')");//����������
		map.put("cert_typ", "decode(cert_typ,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100')");//�߹�֤������
		map.put("cert_code", "cert_code");//�ɶ�֤������
		map.put("invt_name", "invt_name");//�ɶ�����
		map.put("cur_type", "cur_type");//����
		map.put("invt_amt", "invt_amt");//���ʽ��
		map.put("invt_perc", "invt_perc");//���ʱ���
		map.put("invt_desc", "com_invt_desc");//����˵��
		map.put("inv_date", "inv_date");//��������
		map.put("remark", "remark");//��ע
		
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
			String de_cus_corp=" truncate table cus_corp_apital  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement de_ps_cus_corp_v=null;
			String de_cus_corp_v=" DELETE FROM cus_com_rel_Apital@CMISTEST2101 a WHERE a.cus_id='c052841357' AND a.cert_typ='29' AND a.cert_code='1876292'  ";
			de_ps_cus_corp_v=conn.prepareStatement(de_cus_corp_v);
			de_ps_cus_corp_v.execute();
			if(de_ps_cus_corp_v!=null){
				de_ps_cus_corp_v.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO cus_corp_apital ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM cus_com_rel_Apital@CMISTEST2101 b where b.cus_id_rel is not null) ";
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
	
	/*
	 * ����Թ��ͻ��Ʊ� FNC_STAT_BASE 2 FNC_STAT_BASE��ֻ����ͻ���Ϊ��ʽ�ͻ��Ŀͻ���
	 */
	public static void test5(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//�ͻ���
		map.put("STAT_PRD_STYLE", "STAT_PRD_STYLE");//�ͻ���
		map.put("STAT_PRD", "STAT_PRD");//�߹�����'02','03','04','05','10'
		map.put("STAT_BS_STYLE_ID", "STAT_BS_STYLE_ID");//�߹�֤������
		map.put("STAT_PL_STYLE_ID", "STAT_PL_STYLE_ID");//�߹�֤������
		map.put("STAT_CF_STYLE_ID", "STAT_CF_STYLE_ID");//�߹�����
		map.put("STAT_FI_STYLE_ID", "STAT_FI_STYLE_ID");//�Ա�
		map.put("STAT_SOE_STYLE_ID", "STAT_SOE_STYLE_ID");//��������
		map.put("STAT_SL_STYLE_ID", "STAT_SL_STYLE_ID");//ְҵ
		map.put("STAT_ACC_STYLE_ID", "''");//ְ��
		map.put("STAT_DE_STYLE_ID", "''");//ְ��
		map.put("STYLE_ID1", "STYLE_ID1");//���ѧ��,��ֵһ��
		map.put("STYLE_ID2", "STYLE_ID2");//���ѧλ
		map.put("STATE_FLG", "STATE_FLG");//ǩ��������ʼ����
		map.put("STAT_IS_NRPT", "STAT_IS_NRPT");//ǩ��������������
		map.put("STAT_STYLE", "STAT_STYLE");//��Ȩ�鿪ʼ����
		map.put("STAT_IS_AUDIT", "STAT_IS_AUDIT");//��Ȩ�鵽������
		map.put("STAT_ADT_ENTR", "STAT_ADT_ENTR");//��������
		map.put("STAT_ADT_CONC", "STAT_ADT_CONC");//�Ǽ���
		map.put("STAT_IS_ADJT", "STAT_IS_ADJT");//�Ǽǻ���
		map.put("STAT_ADJ_RSN", "STAT_ADJ_RSN");//�Ǽ�����
		map.put("INPUT_ID", "INPUT_ID");//��������
		map.put("INPUT_BR_ID", "INPUT_BR_ID");//����������
		map.put("INPUT_DATE", "INPUT_DATE");//���˱���
		map.put("LAST_UPD_ID", "LAST_UPD_ID");//
		map.put("LAST_UPD_DATE", "LAST_UPD_DATE");//��ϵ�绰
		map.put("FNC_TYPE", "FIN_REP_TYPE");//���񱨱�����
		
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
			String de_cus_corp=" truncate table FNC_STAT_BASE  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO fnc_stat_base ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM fnc_stat_base@CMISTEST2101 b  WHERE B.FIN_REP_TYPE IS NOT NULL and length(b.cus_id)=10 ) ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
//			PreparedStatement r_insert_cus_corp_ps=null;
//			String r_insert_cus_corp=" INSERT INTO cus_corp_mgr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM cus_com_manager@CMISTEST2101 b WHERE b.cus_id_rel is not null and b.com_mrg_typ in ('02','03','04','05','10')) ";
//			System.out.println(r_insert_cus_corp);
//			
//			r_insert_cus_corp_ps=conn.prepareStatement(r_insert_cus_corp);
//			r_insert_cus_corp_ps.execute();
//			if(r_insert_cus_corp_ps!=null){
//				r_insert_cus_corp_ps.close();
//			}
			
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
	
	
	/*
	 * ����Թ��ͻ��Ʊ�(�ʲ���ծ��) FNC_STAT_BS 2 FNC_STAT_BS��ֻ����ͻ���Ϊ��ʽ�ͻ��Ŀͻ���
	 */
	public static void test6(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//�ͻ���
		map.put("STAT_STYLE", "STAT_STYLE");//
		map.put("STAT_YEAR", "STAT_YEAR");//
		map.put("STAT_ITEM_ID", "STAT_ITEM_ID");//
		map.put("STAT_INIT_AMT1", "STAT_INIT_AMT1");//
		map.put("STAT_END_AMT1", "STAT_END_AMT1");//
		map.put("STAT_INIT_AMT2", "STAT_INIT_AMT2");//
		map.put("STAT_END_AMT2", "STAT_END_AMT2");//
		map.put("STAT_INIT_AMT3", "STAT_INIT_AMT3");//
		map.put("STAT_END_AMT3", "STAT_END_AMT3");//
		map.put("STAT_INIT_AMT4", "STAT_INIT_AMT4");//
		map.put("STAT_END_AMT4", "STAT_END_AMT4");//
		map.put("STAT_INIT_AMT5", "STAT_INIT_AMT5");//
		map.put("STAT_END_AMT5", "STAT_END_AMT5");//
		map.put("STAT_INIT_AMT6", "STAT_INIT_AMT6");//
		map.put("STAT_END_AMT6", "STAT_END_AMT6");//
		map.put("STAT_INIT_AMT7", "STAT_INIT_AMT7");//
		map.put("STAT_END_AMT7", "STAT_END_AMT7");//
		map.put("STAT_INIT_AMT8", "STAT_INIT_AMT8");//
		map.put("STAT_END_AMT8", "STAT_END_AMT8");//
		map.put("STAT_INIT_AMT9", "STAT_INIT_AMT9");//
		map.put("STAT_END_AMT9", "STAT_END_AMT9");//
		map.put("STAT_INIT_AMT10", "STAT_INIT_AMT10");//
		map.put("STAT_END_AMT10", "STAT_END_AMT10");//
		map.put("STAT_INIT_AMT11", "STAT_INIT_AMT11");//
		map.put("STAT_END_AMT11", "STAT_END_AMT11");//
		map.put("STAT_INIT_AMT12", "STAT_INIT_AMT12");//
		map.put("STAT_END_AMT12", "STAT_END_AMT12");//
		map.put("STAT_INIT_AMT_Q1", "STAT_INIT_AMT_Q1");//
		map.put("STAT_END_AMT_Q1", "STAT_END_AMT_Q1");//
		map.put("STAT_INIT_AMT_Q2", "STAT_INIT_AMT_Q2");//
		map.put("STAT_END_AMT_Q2", "STAT_END_AMT_Q2");//
		map.put("STAT_INIT_AMT_Q3", "STAT_INIT_AMT_Q3");//
		map.put("STAT_END_AMT_Q3", "STAT_END_AMT_Q3");//
		map.put("STAT_INIT_AMT_Q4", "STAT_INIT_AMT_Q4");//
		map.put("STAT_END_AMT_Q4", "STAT_END_AMT_Q4");//
		
		map.put("STAT_INIT_AMT_Y1", "STAT_INIT_AMT_Y1");//
		map.put("STAT_END_AMT_Y1", "STAT_END_AMT_Y1");//
		map.put("STAT_INIT_AMT_Y2", "STAT_INIT_AMT_Y2");//
		map.put("STAT_END_AMT_Y2", "STAT_END_AMT_Y2");//
		map.put("STAT_INIT_AMT_Y", "STAT_INIT_AMT_Y");//
		map.put("STAT_END_AMT_Y", "STAT_END_AMT_Y");//
		
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
			String de_cus_corp=" truncate table FNC_STAT_BS  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO FNC_STAT_BS ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM FNC_STAT_BS@CMISTEST2101 b ) ";
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
	
	/*
	 * ����Թ��ͻ��Ʊ�(�����) Fnc_Stat_Is 2 Fnc_Stat_Is��ֻ����ͻ���Ϊ��ʽ�ͻ��Ŀͻ���
	 */
	public static void test7(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//�ͻ���
		map.put("STAT_STYLE", "STAT_STYLE");//
		map.put("STAT_YEAR", "STAT_YEAR");//
		map.put("STAT_ITEM_ID", "STAT_ITEM_ID");//
		map.put("STAT_INIT_AMT1", "STAT_INIT_AMT1");//
		map.put("STAT_END_AMT1", "STAT_END_AMT1");//
		map.put("STAT_INIT_AMT2", "STAT_INIT_AMT2");//
		map.put("STAT_END_AMT2", "STAT_END_AMT2");//
		map.put("STAT_INIT_AMT3", "STAT_INIT_AMT3");//
		map.put("STAT_END_AMT3", "STAT_END_AMT3");//
		map.put("STAT_INIT_AMT4", "STAT_INIT_AMT4");//
		map.put("STAT_END_AMT4", "STAT_END_AMT4");//
		map.put("STAT_INIT_AMT5", "STAT_INIT_AMT5");//
		map.put("STAT_END_AMT5", "STAT_END_AMT5");//
		map.put("STAT_INIT_AMT6", "STAT_INIT_AMT6");//
		map.put("STAT_END_AMT6", "STAT_END_AMT6");//
		map.put("STAT_INIT_AMT7", "STAT_INIT_AMT7");//
		map.put("STAT_END_AMT7", "STAT_END_AMT7");//
		map.put("STAT_INIT_AMT8", "STAT_INIT_AMT8");//
		map.put("STAT_END_AMT8", "STAT_END_AMT8");//
		map.put("STAT_INIT_AMT9", "STAT_INIT_AMT9");//
		map.put("STAT_END_AMT9", "STAT_END_AMT9");//
		map.put("STAT_INIT_AMT10", "STAT_INIT_AMT10");//
		map.put("STAT_END_AMT10", "STAT_END_AMT10");//
		map.put("STAT_INIT_AMT11", "STAT_INIT_AMT11");//
		map.put("STAT_END_AMT11", "STAT_END_AMT11");//
		map.put("STAT_INIT_AMT12", "STAT_INIT_AMT12");//
		map.put("STAT_END_AMT12", "STAT_END_AMT12");//
		map.put("STAT_INIT_AMT_Q1", "STAT_INIT_AMT_Q1");//
		map.put("STAT_END_AMT_Q1", "STAT_END_AMT_Q1");//
		map.put("STAT_INIT_AMT_Q2", "STAT_INIT_AMT_Q2");//
		map.put("STAT_END_AMT_Q2", "STAT_END_AMT_Q2");//
		map.put("STAT_INIT_AMT_Q3", "STAT_INIT_AMT_Q3");//
		map.put("STAT_END_AMT_Q3", "STAT_END_AMT_Q3");//
		map.put("STAT_INIT_AMT_Q4", "STAT_INIT_AMT_Q4");//
		map.put("STAT_END_AMT_Q4", "STAT_END_AMT_Q4");//
		
		map.put("STAT_INIT_AMT_Y1", "STAT_INIT_AMT_Y1");//
		map.put("STAT_END_AMT_Y1", "STAT_END_AMT_Y1");//
		map.put("STAT_INIT_AMT_Y2", "STAT_INIT_AMT_Y2");//
		map.put("STAT_END_AMT_Y2", "STAT_END_AMT_Y2");//
		map.put("STAT_INIT_AMT_Y", "STAT_INIT_AMT_Y");//
		map.put("STAT_END_AMT_Y", "STAT_END_AMT_Y");//
		
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
			String de_cus_corp=" truncate table Fnc_Stat_Is  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Fnc_Stat_Is ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Fnc_Stat_Is@CMISTEST2101 b ) ";
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
	
	/*
	 * ����Թ��ͻ��Ʊ�(�ֽ�������) Fnc_Stat_Cfs 2 Fnc_Stat_Cfs��ֻ����ͻ���Ϊ��ʽ�ͻ��Ŀͻ���
	 */
	public static void test8(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//�ͻ���
		map.put("STAT_STYLE", "STAT_STYLE");//
		map.put("STAT_YEAR", "STAT_YEAR");//
		map.put("STAT_ITEM_ID", "STAT_ITEM_ID");//
		map.put("STAT_INIT_AMT1", "STAT_INIT_AMT1");//
		map.put("STAT_END_AMT1", "STAT_END_AMT1");//
		map.put("STAT_INIT_AMT2", "STAT_INIT_AMT2");//
		map.put("STAT_END_AMT2", "STAT_END_AMT2");//
		map.put("STAT_INIT_AMT3", "STAT_INIT_AMT3");//
		map.put("STAT_END_AMT3", "STAT_END_AMT3");//
		map.put("STAT_INIT_AMT4", "STAT_INIT_AMT4");//
		map.put("STAT_END_AMT4", "STAT_END_AMT4");//
		map.put("STAT_INIT_AMT5", "STAT_INIT_AMT5");//
		map.put("STAT_END_AMT5", "STAT_END_AMT5");//
		map.put("STAT_INIT_AMT6", "STAT_INIT_AMT6");//
		map.put("STAT_END_AMT6", "STAT_END_AMT6");//
		map.put("STAT_INIT_AMT7", "STAT_INIT_AMT7");//
		map.put("STAT_END_AMT7", "STAT_END_AMT7");//
		map.put("STAT_INIT_AMT8", "STAT_INIT_AMT8");//
		map.put("STAT_END_AMT8", "STAT_END_AMT8");//
		map.put("STAT_INIT_AMT9", "STAT_INIT_AMT9");//
		map.put("STAT_END_AMT9", "STAT_END_AMT9");//
		map.put("STAT_INIT_AMT10", "STAT_INIT_AMT10");//
		map.put("STAT_END_AMT10", "STAT_END_AMT10");//
		map.put("STAT_INIT_AMT11", "STAT_INIT_AMT11");//
		map.put("STAT_END_AMT11", "STAT_END_AMT11");//
		map.put("STAT_INIT_AMT12", "STAT_INIT_AMT12");//
		map.put("STAT_END_AMT12", "STAT_END_AMT12");//
		map.put("STAT_INIT_AMT_Q1", "STAT_INIT_AMT_Q1");//
		map.put("STAT_END_AMT_Q1", "STAT_END_AMT_Q1");//
		map.put("STAT_INIT_AMT_Q2", "STAT_INIT_AMT_Q2");//
		map.put("STAT_END_AMT_Q2", "STAT_END_AMT_Q2");//
		map.put("STAT_INIT_AMT_Q3", "STAT_INIT_AMT_Q3");//
		map.put("STAT_END_AMT_Q3", "STAT_END_AMT_Q3");//
		map.put("STAT_INIT_AMT_Q4", "STAT_INIT_AMT_Q4");//
		map.put("STAT_END_AMT_Q4", "STAT_END_AMT_Q4");//
		
		map.put("STAT_INIT_AMT_Y1", "STAT_INIT_AMT_Y1");//
		map.put("STAT_END_AMT_Y1", "STAT_END_AMT_Y1");//
		map.put("STAT_INIT_AMT_Y2", "STAT_INIT_AMT_Y2");//
		map.put("STAT_END_AMT_Y2", "STAT_END_AMT_Y2");//
		map.put("STAT_INIT_AMT_Y", "STAT_INIT_AMT_Y");//
		map.put("STAT_END_AMT_Y", "STAT_END_AMT_Y");//
		
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
			String de_cus_corp=" truncate table Fnc_Stat_Cfs  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Fnc_Stat_Cfs ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Fnc_Stat_Cfs@CMISTEST2101 b ) ";
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
	
	/*
	 * ����Թ��ͻ��Ʊ�(����ָ���) Fnc_Index_Rpt 2 Fnc_Index_Rpt��ֻ����ͻ���Ϊ��ʽ�ͻ��Ŀͻ���
	 */
	public static void test9(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//�ͻ���
		map.put("STAT_STYLE", "STAT_STYLE");//
		map.put("STAT_YEAR", "STAT_YEAR");//
		map.put("STAT_ITEM_ID", "STAT_ITEM_ID");//
		map.put("STAT_INIT_AMT1", "STAT_INIT_AMT1");//
		map.put("STAT_INIT_AMT2", "STAT_INIT_AMT2");//
		map.put("STAT_INIT_AMT3", "STAT_INIT_AMT3");//
		map.put("STAT_INIT_AMT4", "STAT_INIT_AMT4");//
		map.put("STAT_INIT_AMT5", "STAT_INIT_AMT5");//
		map.put("STAT_INIT_AMT6", "STAT_INIT_AMT6");//
		map.put("STAT_INIT_AMT7", "STAT_INIT_AMT7");//
		map.put("STAT_INIT_AMT8", "STAT_INIT_AMT8");//
		map.put("STAT_INIT_AMT9", "STAT_INIT_AMT9");//
		map.put("STAT_INIT_AMT10", "STAT_INIT_AMT10");//
		map.put("STAT_INIT_AMT11", "STAT_INIT_AMT11");//
		map.put("STAT_INIT_AMT12", "STAT_INIT_AMT12");//
		map.put("STAT_INIT_AMT_Q1", "STAT_INIT_AMT_Q1");//
		map.put("STAT_INIT_AMT_Q2", "STAT_INIT_AMT_Q2");//
		map.put("STAT_INIT_AMT_Q3", "STAT_INIT_AMT_Q3");//
		map.put("STAT_INIT_AMT_Q4", "STAT_INIT_AMT_Q4");//		
		map.put("STAT_INIT_AMT_Y1", "STAT_INIT_AMT_Y1");//
		map.put("STAT_INIT_AMT_Y2", "STAT_INIT_AMT_Y2");//
		map.put("STAT_INIT_AMT_Y", "STAT_INIT_AMT_Y");//
		
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
			String de_cus_corp=" truncate table Fnc_Index_Rpt  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Fnc_Index_Rpt ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Fnc_Index_Rpt@CMISTEST2101 b ) ";
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
}
