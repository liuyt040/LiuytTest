package ncmis.data.B_cus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * 导入对公客户基本信息cus_corp,缺少：法人证件发证机，财务报表类型
 * 导入对公客户与客户经理关系表cus_loan_rel
 * 导入对公客户高管信息 cus_corp_mgr,高管的主键需要修改，有cus_id、CUS_REL_ID改为cus_id、CUS_REL_ID、MRG_TYPE
 * 导入对公客户资本构成信息
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

	//导入对公客户基本信息
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//客户号
		map.put("trans_cus_id", "cus_id");//客户号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");//客户类型
		map.put("cert_type", "decode(cert_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','29900')");//证件类型
		map.put("cert_code", "cert_code");//证件号码
		map.put("cus_status", "decode(cus_status,'00','00','20','20','01','01','02','00','03','20')");//客户状态
		
		map.put("cus_short_name", "cus_short_name");//客户简称
		map.put("cus_en_name", "cus_en_name");//外文名称
		map.put("country", "com_country");//国别
		map.put("com_lcl_flg", "decode(com_lcl_flg,'1','Y','2','N','')");//是否异地
		map.put("city_flg", "decode(com_city_flg,'10','00','11','01','20','02','21','03','')");//城乡类型
		map.put("invest_type", "invest_type");//投资主体,字典一致，不用映射
		map.put("sub_typ", "decode(com_sub_typ,'1','01','2','04','3','04','4','04','5','04','7','02','8','03','6','04')");//隶属关系 落标
		map.put("hold_type", "decode(com_hold_type,'111','010','112','020','121','030','122','040','171','050','172','060','201','070','202','080','301','090','302','100','110')");//落标
		map.put("cll_type", "com_cll_type");//行业分类
		map.put("cll_name", "com_cll_name");//行业分类名称
		map.put("in_reg_state_code", "in_reg_state_code");//行业区域分类
		map.put("in_cll_type", "in_cll_type");//行业行业分类
		map.put("str_date", "com_str_date");//成立日期
		map.put("employee", "com_employee");//从业人数
		map.put("scale_cll_type", "decode(com_sta_type,'1','00','2','01','3','02','4','03','5','04','6','05','7','09','8','06','9','07','10','08','11','11','12','12','13','10','14','13','15','14','16','15','')");//企业规模行业类型（银监会）
		map.put("sal_volume", "com_sales_amt/10000");//销售额（万元），除以10000
		map.put("oper_income", "com_sales_amt/10000");//营业收入（万元，除以10000
		map.put("ass_total", "com_sum_amt/10000");//资产总额（万元），除以10000
		map.put("cus_scale", "com_scale");//企业规模
		map.put("is_trade_enterprises", "decode(is_trade_enterprises,'1','Y','N')");//是否贸易企业
		map.put("is_green_market", "decode(is_green_market,'1','Y','N')");//是否绿色信贷企业
		map.put("technology_flag", "technology_flag");//科技科创企业标识
//		map.put("nat_eco_sec", "technology_flag");//国民经济部门编号
//		map.put("unit_alle_ind", "");//产业扶贫单位标
//		map.put("alle_bunb", "");//扶贫带动人数		
//		map.put("fram_ind", "");//农场标志	
//		map.put("farmer_cop_ind", "");//农民专业合作社标志		
//		map.put("agr_ind", "");//是否农村集体经济组织	
		map.put("ins_code", "com_ins_code");//组织机构代码
		map.put("ins_reg_dt", "com_ins_reg_date");//组织机构登记日期
		map.put("ins_reg_end_dt", "com_ins_exp_date");//组织机构登记有效期
		map.put("ins_org", "com_ins_org");//组织机构代码证颁发机关
		map.put("ins_ann_date", "com_ins_ann_date");//组织机构代码证年检到期日期
//		map.put("license_type", "reg_type");//注册登记号类型
		map.put("reg_code", "reg_code");//注册登记号
		map.put("reg_type", "decode(reg_type,'110','1110','120','1120','130','1130','141','1131','142','1132','143','1133','149','1139','151','1140','159','1149','160','1150','171','1171','172','1172','173','1173','174','1174','175','1300','176','1200','210','1210','220','1220','230','1230','240','1240','310','1310','320','1320','330','1330','340','1340','410','1175','420','1175','510','9000','520','4000','530','9000','540','9000','550','9000','560','2000','570','3400','580','9000')");//注册登记类型
		map.put("admin_org", "admin_org");//主管单位
		map.put("appr_org", "appr_org");//批准机关
		map.put("appr_doc_no", "appr_doc_no");//批准文号
//		map.put("reg_province", "");//注册登记地址-省
//		map.put("reg_city", "");//注册登记地址-市
		map.put("reg_area", "reg_state_code");//注册登记地址-县/区
//		map.put("reg_town", "");//注册登记地址-乡/镇
//		map.put("reg_country", "");//注册登记地址-村
		map.put("reg_addr", "reg_addr");//详细地址
		map.put("en_reg_addr", "en_reg_addr");//外文注册登记地址
//		map.put("acu_province", "");//实际经营地址-省
//		map.put("acu_city", "");//实际经营地址-市
//		map.put("acu_area", "");//实际经营地址-县/区
//		map.put("acu_town", "");//实际经营地址-乡/镇
//		map.put("acu_country", "");//实际经营地址-村
		map.put("acu_addr", "acu_addr");//实际经营地址-详细地址
		map.put("main_opt_scp", "com_main_opt_scp");//主营业务范围
		map.put("part_opt_scp", "com_part_opt_scp");//兼营业务范围
		map.put("reg_cur_type", "reg_cur_type");//注册资本/开办资金币种
		map.put("reg_cap_amt", "reg_cap_amt/10000");//注册资本金额（万元），除以10000
		map.put("paid_cap_cur_type", "reg_cur_type");//实收资本币种，与老系统注册币种一致
		map.put("paid_cap_amt", "paid_cap_amt/10000");//实收资本(万元)，除以10000
		map.put("reg_dt", "reg_start_date");//注册登记日期
		map.put("reg_end_dt", "reg_end_date");//注册登记有效期
		map.put("ann_date", "reg_audit_end_date");//年检到期日
//		map.put("reg_area_code2", "reg_audit_end_date");//注册地区域编码
//		map.put("reg_audit_date", "");//注册登记年审日期
		map.put("loc_tax_reg_code", "loc_tax_reg_code");//地税税务登记代码
		map.put("loc_tax_reg_dt", "loc_tax_reg_dt");//地税税务登记日期
		map.put("loc_tax_reg_end_dt", "loc_tax_reg_end_dt");//地税税务登记有效期
		map.put("loc_tax_ann_date", "reg_audit_end_date");//地税年检到期日
		map.put("loc_tax_reg_org", "loc_tax_reg_org");//地税登记机关
		map.put("nat_tax_reg_code", "nat_tax_reg_code");//国税税务登记代码
		map.put("nat_tax_reg_dt", "nat_tax_reg_dt");//国税税务登记日期
		map.put("nat_tax_reg_end_dt", "nat_tax_reg_end_dt");//国税税务登记有效期
		map.put("nat_tax_ann_date", "reg_audit_end_date");//国税年检到期日
		map.put("nat_tax_reg_org", "nat_tax_reg_org");//国税登记机关
		map.put("com_sp_business", "decode(com_sp_business,'1','Y','N')");//特种经营标识
		map.put("com_sp_lic_no", "com_sp_lic_no");//特种经营标识
		map.put("com_sp_detail", "com_sp_detail");//特种经营情况
		map.put("com_sp_lic_org", "com_sp_lic_org");//特种许可证颁发机关
		map.put("com_sp_str_date", "com_sp_str_date");//特种经营登记日期
		map.put("com_sp_end_date", "com_sp_end_date");//特种经营到期日期
		
		//贷款卡信息
		map.put("loan_card_flg", "decode(loan_card_flg,'1','1','2','0','')");//有无贷款卡
		map.put("loan_card_id", "loan_card_id");//贷款卡号
		map.put("loan_card_pwd", "loan_card_pwd");//贷款卡密码
		map.put("loan_card_eff_flg", "decode(loan_card_eff_flg,'01','1','03','2','3')");//贷款卡状态
		map.put("loan_card_end_date", "loan_card_audit_dt");//贷款卡年检到期日
		
		map.put("taxpayer_code", "decode(cert_type,'29',cert_code,'')");//纳税人识别号
//		map.put("deft_flag", "");//违约标示
//		map.put("deft_date", "");//违约日期
//		map.put("deft_days", "");//违约天数
		
		//银行账户信息
		map.put("bas_acc_flg", "decode(bas_acc_flg,'1','Y','2','N','')");//基本存款账户是否在本机构
		map.put("bas_acc_licence", "bas_acc_licence");//基本存款账户开户许可证
		map.put("bas_acc_bank", "bas_acc_bank");//基本存款账户开户行
		map.put("bas_acc_no", "bas_acc_no");//基本存款账户账号
		map.put("bas_acc_date", "bas_acc_date");//基本账户开户日期
		map.put("bas_acc_date", "bas_acc_date");//基本账户开户日期
		
		//法定代表人及其配偶信息
		map.put("legal_cert_type", "decode(legal_cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//法定代表人证件类型
		map.put("legal_cert_code", "legal_cert_code");//法定代表人证件号码
		map.put("reg_org_legal", "reg_org_legal");//法定代表人证件号码
		map.put("legal_no", "cus_id_rel");//法定代表人客户编号
		map.put("legal_name", "legal_name");//法定代表人姓名
		map.put("legal_sex", "''");//法定代表人性别,老系统没有
		map.put("legal_resume", "cus_work_resume");//法定代表人个人简历
		map.put("legal_sign_init_date", "legal_sign_init_date");//法定代表人签字样本开始日期
		map.put("legal_sign_end_date", "legal_end_init_date");//法定代表人签字样本结束日期
		map.put("legal_phone", "mobile");//法定代表人手机号
//		map.put("legal_mar_st", "");//法定代表人婚姻情况
		map.put("legal_sps_cert_type", "decode(legal_sps_id_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//法定代表人配偶证件类型
		map.put("legal_sps_cert_code", "legal_sps_id_code");//法定代表人配偶证件号码
		map.put("legal_sps_no", "legal_sps_no");//法定代表人配偶客户编码
		map.put("legal_sps_name", "legal_sps_name");//法定代表人配偶姓名
//		map.put("legal_sps_phone", "");//法定代表人配偶手机号
		
		//实际控制人信息
		map.put("real_conl_cert_type", "decode(reality_controler_id_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100','')");//实际控制人证件类型,需要转化
		map.put("real_conl_cert_code", "reality_controler_id_code");//实际控制人证件号码
		map.put("real_conl_no", "reality_controler_no");//实际控制人客户编
		map.put("real_conl_name", "reality_controler_name");//实际控制人姓名
		map.put("real_sign_init_date", "real_sign_init_date");//实际控制人签字样本开始日期
		map.put("real_sign_end_date", "real_sign_end_date");//实际控制人签字样本结束日期
		map.put("real_accredit_init_date", "real_accredit_init_date");//实际控制人授权书开始日期
		map.put("real_accredit_end_date", "real_accredit_end_date");//实际控制人授权书结束日期
		map.put("real_work_resume", "real_work_resume");//实际控制人个人简历
//		map.put("real_conl_phone", "legal_sps_name");//实际控制人手机号
//		map.put("real_mar_st", "legal_sps_name");//实际控制人婚姻情况
		map.put("real_sps_cert_type", "decode(reality_sps_id_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100','')");//实际控制人配偶证件类型
		map.put("real_sps_cert_code", "reality_id_code");//实际控制人配偶证件号码
		map.put("real_sps_no", "reality_sps_no");//实际控制人配偶客户编号
		map.put("real_sps_name", "reality_sps_name");//实际控制人配偶姓名
//		map.put("real_sps_phone", "legal_sps_name");//实际控制人配偶手机号
		
		//财务信息
		map.put("fna_mgr", "fna_mgr");//财务部负责人
//		map.put("fna_telephone", "reality_controler_id_code");//财务部固定电话
		map.put("fna_contact", "com_operator");//财务部联系人
//		map.put("fna_con_phone", "reality_controler_name");//财务部联系人固话
		map.put("fna_con_mobile_phone", "phone");//财务部联系人手机号
		
		//地址信息
		map.put("post_addr", "post_addr");//详细地址
		map.put("phone", "legal_phone");//手机号码
//		map.put("landlin_telephone", "");//固定电话
		map.put("fax_code", "fax_code");//传真
		map.put("email", "email");//Email
		map.put("web_url", "web_url");//网址
		map.put("post_code", "post_code");//邮政编码
		
		//概况信息
		map.put("mrk_flg", "decode(com_mrk_flg,'1','Y','2','N','')");//是否上市
		map.put("mrk_area", "com_mrk_area");//上市地
		map.put("stock_id", "com_stock_id");//股票代码
		map.put("is_gov_plat", "decode(is_government,'1','Y','2','N','')");//是否政府融资平台is_government
		map.put("imp_exp_flg", "decode(com_imp_exp_flg,'1','Y','2','N','')");//是否拥有进出口权
		map.put("fexc_prm_code", "com_fexc_prm_code");//外汇许可证号
		map.put("main_product", "com_main_product");//主要生产情况
		map.put("prod_equip", "com_prod_equip");//主要生产设备
		map.put("fact_prod", "com_fact_prod");//主要生产能力
		map.put("opt_aera", "com_opt_aera");//经营场地面积(平方米)
		map.put("opt_owner", "decode(com_opt_owner,'1','01','2','02','3','99')");//经营场地所有权，码值一致
		map.put("opt_st", "decode(com_opt_st,'100','000','200','001','002')");//经营状况
		map.put("imptt_flg", "decode(com_imptt_flg,'1','Y','2','N','')");//地区重点企业
		map.put("prep_flg", "decode(com_prep_flg,'1','Y','2','N','')");//优势企业
//		map.put("is_hi_tech", "");//高新企业
		map.put("dhgh_flg", "decode(com_dhgh_flg,'1','Y','2','N','')");//高环境污染风险企业
//		map.put("ind_stru_adj_type", "reality_controler_id_type");//产业结构调整类型
		map.put("ci_entp", "decode(com_cl_entp,'1','Y','2','N','')");//国家宏观调控限控产业
//		map.put("ind_trans_flag", "");//工业转型升级标志
//		map.put("stra_ind_type", "reality_controler_name");//战略新兴产业类型
		map.put("hd_enterprise", "decode(com_hd_enterprise,'1','Y','2','N','')");//龙头企业
//		map.put("is_new_corp", "");//是否新建企业
//		map.put("main_scp_country", "");//主要业务所在国家
//		map.put("mng_org", "");//上级主管单位
		
//		map.put("fin_rep_type", "");//财务报表类型待处理
		
		//合作信息
		map.put("cus_bank_rel", "decode(cus_bank_rel,'B1','B1','A2','A2','C6','A2','C7','A2','C8','A2','C9','A2','D2','A2','D3','A2','D8','A2','D9','A2','B4','A3','B6','A3','A5','A5','')");//客户信贷关系
//		map.put("rel_stat_adj_date", "com_mrk_area");//关系状态调整时间
//		map.put("rel_stat_adj_reason", "com_stock_id");//关系调整原因
		map.put("init_loan_date", "com_init_loan_date");//建立信贷关系时间
		map.put("crd_grade", "decode(com_crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//信用等级(内部)
		map.put("crd_dt", "com_crd_dt");//信用评定日期
		map.put("out_crd_grade", "decode(com_out_crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//信用等级(外部)
		map.put("out_crd_dt", "com_out_crd_dt");//信用评定日期(外部)
		map.put("out_crd_org", "com_out_crd_org");//评定机构(外部)
		
		//管护人
		map.put("cust_mgr", "decode(cust_mgr,'',input_id,cust_mgr)");
//		map.put("main_br_id", "SELECT a.usr_bch FROM s_usr a WHERE a.usr_cde=''");//主管机构
		map.put("input_id", "input_id");//登记人
		map.put("input_br_id", "input_br_id");//登记机构
		map.put("input_date", "input_date");//登记日期
		map.put("last_chg_usr", "last_upd_id");//更新人
		map.put("last_chg_dt", "last_upd_date");//更新日期
		map.put("instu_cde", "'0000'");//法人编码
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
/*
 * map.put("fin_rep_type", "");//财务报表类型待处理
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
			//更新法定代表人性别，根据法定代表人身份证号判断，倒数第二位，单数为男，双数为女
			PreparedStatement up_legal_sex_ps=null;
			String legal_sex_ps=" UPDATE cus_corp a SET a.legal_sex = ( SELECT decode(mod(substr(c.legal_cert_code,17,1),2),0,'2',1,'1','3') FROM cus_corp c WHERE c.cus_id=a.cus_id) WHERE a.legal_cert_type='10100' AND length(legal_cert_code)=18 ";
			up_legal_sex_ps=conn.prepareStatement(legal_sex_ps);
			up_legal_sex_ps.execute();
			if(up_legal_sex_ps!=null){
				up_legal_sex_ps.close();
			}
			//更新行政区划-市
			PreparedStatement up_reg_city_ps=null;
			String reg_city_ps=" UPDATE cus_corp a SET a.reg_city=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.reg_area) ";
			up_reg_city_ps=conn.prepareStatement(reg_city_ps);
			up_reg_city_ps.execute();
			if(up_reg_city_ps!=null){
				up_reg_city_ps.close();
			}
			
			//更新行政区划-省
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
			//将国别为空的数据默认为中国
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
	
	//导入对公客户关系表cus_loan_rel 
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("PK_ID", "cus_id");//客户号
		map.put("cus_id", "cus_id");//客户号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");//客户类型
		map.put("cus_short_name", "cus_short_name");//客户简称
		map.put("cus_en_name", "cus_en_name");//外文名称
		map.put("cert_type", "decode(cert_type,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//证件类型
		map.put("cert_code", "cert_code");//证件号码
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
	 * 导入对公客户高管信息表cus_corp_mgr 
	 * 1 老系统高管客户号为空，不移
	 * 2 老系统高管类别对新类别存在多对一，导致主键冲突，先移一一对应的'02','03','04','05','10'，再移其他，
	 */
	public static void test3(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//客户号
		map.put("cus_rel_id", "cus_id_rel");//客户号
		map.put("mrg_type", "decode(com_mrg_typ,'02','05','03','02','04','03','05','06','10','07','08')");//高管类型'02','03','04','05','10'
		map.put("mrg_cert_type", "decode(com_mrg_cert_typ,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100')");//高管证件类型
		map.put("mrg_cert_code", "com_mrg_cert_code");//高管证件号码
		map.put("mrg_name", "com_mrg_name");//高管名称
		map.put("mrg_sex", "com_mrg_sex");//性别
		map.put("mrg_bday", "com_mrg_bday");//出生日期
		map.put("mrg_occ", "decode(com_mrg_occ,'0','01','1','02','3','03','4','04','5','05','6','06','8','01','X','07','Y','08','Z','09','09')");//职业
		map.put("MRG_DUTY", "decode(com_mrg_duty,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//职务
		map.put("MRG_CRTF", "decode(com_mrg_crtf,'0','01','1','02','2','03','3','04','9','01')");//职称
		map.put("MRG_EDT", "com_mrg_edt");//最高学历,码值一致
		map.put("MRG_DGR", "decode(com_mrg_dgr,'1','0','2','1','3','2','4','3','9','5','0','4')");//最高学位
		map.put("SIGN_INIT_DATE", "sign_init_date");//签字样本开始日期
		map.put("SIGN_END_DATE", "sign_end_date");//签字样本到期日期
		map.put("ACCREDIT_INIT_DATE", "accredit_init_date");//授权书开始日期
		map.put("ACCREDIT_END_DATE", "accredit_end_date");//授权书到期日期
		map.put("RESUME", "remark");//工作简历
		map.put("INPUT_ID", "input_id");//登记人
		map.put("INPUT_BR_ID", "input_br_id");//登记机构
		map.put("INPUT_DATE", "input_date");//登记日期
		map.put("LAST_CHG_USR", "last_upd_id");//最后更新人
		map.put("LAST_CHG_DT", "last_upd_date");//最后更新日期
		map.put("INSTU_CDE", "'0000'");//法人编码
//		map.put("REMARK", "");//
		map.put("MRG_PHONE", "com_mrg_mphn1");//联系电话
		
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
	 * 导入对公客户资本构成信息表cus_corp_apital 
	 * 1 老系统高管客户号为空，不移
	 * 2 老系统高管类别对新类别存在多对一，导致主键冲突，先移一一对应的'02','03','04','05','10'，再移其他，
	 */
	public static void test4(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//客户号
		map.put("cus_rel_id", "cus_id_rel");//客户号
		map.put("invt_typ", "decode(invt_typ,'1','1','2')");//出资人性质
		map.put("cert_typ", "decode(cert_typ,'20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10','10100')");//高管证件类型
		map.put("cert_code", "cert_code");//股东证件号码
		map.put("invt_name", "invt_name");//股东名称
		map.put("cur_type", "cur_type");//币种
		map.put("invt_amt", "invt_amt");//出资金额
		map.put("invt_perc", "invt_perc");//出资比例
		map.put("invt_desc", "com_invt_desc");//出资说明
		map.put("inv_date", "inv_date");//出资日期
		map.put("remark", "remark");//备注
		
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
	 * 导入对公客户财报 FNC_STAT_BASE 2 FNC_STAT_BASE，只导入客户号为正式客户的客户号
	 */
	public static void test5(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//客户号
		map.put("STAT_PRD_STYLE", "STAT_PRD_STYLE");//客户号
		map.put("STAT_PRD", "STAT_PRD");//高管类型'02','03','04','05','10'
		map.put("STAT_BS_STYLE_ID", "STAT_BS_STYLE_ID");//高管证件类型
		map.put("STAT_PL_STYLE_ID", "STAT_PL_STYLE_ID");//高管证件号码
		map.put("STAT_CF_STYLE_ID", "STAT_CF_STYLE_ID");//高管名称
		map.put("STAT_FI_STYLE_ID", "STAT_FI_STYLE_ID");//性别
		map.put("STAT_SOE_STYLE_ID", "STAT_SOE_STYLE_ID");//出生日期
		map.put("STAT_SL_STYLE_ID", "STAT_SL_STYLE_ID");//职业
		map.put("STAT_ACC_STYLE_ID", "''");//职务
		map.put("STAT_DE_STYLE_ID", "''");//职称
		map.put("STYLE_ID1", "STYLE_ID1");//最高学历,码值一致
		map.put("STYLE_ID2", "STYLE_ID2");//最高学位
		map.put("STATE_FLG", "STATE_FLG");//签字样本开始日期
		map.put("STAT_IS_NRPT", "STAT_IS_NRPT");//签字样本到期日期
		map.put("STAT_STYLE", "STAT_STYLE");//授权书开始日期
		map.put("STAT_IS_AUDIT", "STAT_IS_AUDIT");//授权书到期日期
		map.put("STAT_ADT_ENTR", "STAT_ADT_ENTR");//工作简历
		map.put("STAT_ADT_CONC", "STAT_ADT_CONC");//登记人
		map.put("STAT_IS_ADJT", "STAT_IS_ADJT");//登记机构
		map.put("STAT_ADJ_RSN", "STAT_ADJ_RSN");//登记日期
		map.put("INPUT_ID", "INPUT_ID");//最后更新人
		map.put("INPUT_BR_ID", "INPUT_BR_ID");//最后更新日期
		map.put("INPUT_DATE", "INPUT_DATE");//法人编码
		map.put("LAST_UPD_ID", "LAST_UPD_ID");//
		map.put("LAST_UPD_DATE", "LAST_UPD_DATE");//联系电话
		map.put("FNC_TYPE", "FIN_REP_TYPE");//财务报表类型
		
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
	 * 导入对公客户财报(资产负债表) FNC_STAT_BS 2 FNC_STAT_BS，只导入客户号为正式客户的客户号
	 */
	public static void test6(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//客户号
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
	 * 导入对公客户财报(损益表) Fnc_Stat_Is 2 Fnc_Stat_Is，只导入客户号为正式客户的客户号
	 */
	public static void test7(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//客户号
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
	 * 导入对公客户财报(现金流量表) Fnc_Stat_Cfs 2 Fnc_Stat_Cfs，只导入客户号为正式客户的客户号
	 */
	public static void test8(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//客户号
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
	 * 导入对公客户财报(财务指标表) Fnc_Index_Rpt 2 Fnc_Index_Rpt，只导入客户号为正式客户的客户号
	 */
	public static void test9(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("CUS_ID", "cus_id");//客户号
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
