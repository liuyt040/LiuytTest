package ncmis.data.B_cus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * 导入个人客户基本信息cus_corp,缺少：法人证件发证机，财务报表类型
 * 导入个人客户与客户经理关系表cus_loan_rel
 * 注意万元和元的转换
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

	//导入个人客户基本信息
	/*
	 * SELECT indiv_sps_name,cus_id,input_id FROM cus_indiv@CMISTEST2101 WHERE length(indiv_sps_name)>5,配偶客户姓名超长需要过滤
	 */
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("cus_id", "cus_id");//客户号
		map.put("cus_name", "cus_name");//客户名称
		map.put("trans_cus_id", "cus_id");//核心客户号
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250','120','111','130','113','110','114','')");//客户类型
		map.put("indiv_sex", "indiv_sex");//性别
		map.put("cert_type", "decode(cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','10509')");//证件类型
		map.put("cert_code", "cert_code");//证件号码
		map.put("indiv_id_beg_dt", "indiv_id_sta_dt");//证件起始日，为空，不移
		map.put("indiv_id_exp_dt", "indiv_id_exp_dt");//证件有效期
		map.put("agri_flg", "decode(agri_flg,'1','Y','2','N','')");//是否农户
		//**合作信息
		map.put("cus_bank_rel", "cus_bank_rel");//客户信贷关系
		map.put("HOLD_STK_AMT", "com_hold_stk_amt");//拥有我行股份金额
		map.put("bank_duty", "decode(bank_duty,'1','00','2','01','3','02','4','04','5','03')");//在我行职务
		map.put("indiv_ntn", "indiv_ntn");//名族，按老系统字典项进行更新，不需要转换
		map.put("country", "indiv_country");//国别，按老系统字典项进行更新，不需要转换
		map.put("indiv_dt_of_birth", "indiv_dt_of_birth");//出生日期
		//***户籍地址，五联联动
		map.put("brt_province", "''");//户籍地址-省
		map.put("brt_city", "''");//户籍地址-市
		map.put("brt_area", "''");//户籍地址-区县
		map.put("brt_town", "''");//户籍地址-乡/镇
		map.put("brt_country", "''");//户籍地址-村
		
		map.put("family_income", "family_income");//户籍地址,详细地址
		map.put("indiv_brt_place", "indiv_houh_reg_add");//户籍地址,详细地址
		map.put("indiv_pol_st", "decode(indiv_pol_st,'09','05',indiv_pol_st)");//政治面貌
		map.put("indiv_edt", "decode(indiv_edt,'50','40',indiv_edt)");//最高学历,新信贷缺少【50技术学院】
		map.put("indiv_dgr", "decode(indiv_dgr,'9','5','5')");//最高学位
		map.put("indiv_heal_st", "indiv_heal_st");//健康状况
		map.put("indiv_soc_scr", "decode(indiv_soc_scr,'01','00','02','05','03','01','04','03','05','02','06','04','06')");//社会保障情况
		map.put("indiv_hobby", "indiv_hobby");//爱好
		map.put("indiv_mar_st", "decode(indiv_mar_st,'10','00','20','01','21','02','22','03','23','04','30','05','40','06','90','07')");//婚姻状况
		map.put("indiv_sps_cert_type", "decode(indiv_sps_id_typ,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//配偶证件类型
		map.put("indiv_sps_cert_code", "indiv_sps_id_code");//配偶证件号码
		map.put("cus_rel_id", "cus_id_rel");//配偶客户号
		map.put("cus_rel_name", "indiv_sps_name");//配偶客户姓名
		map.put("indiv_sps_mar_code", "indiv_sps_mar_code");//结婚证号(户口簿号)
		map.put("indiv_sps_occ", "decode(indiv_sps_occ,'0','10000','1','20000','3','30000','4','40000','5','50000','6','60000','7','80100','8','10000','X','70000','Y','80000','Z','80000','80100')");//配偶职业
		map.put("indiv_scom_name", "indiv_scom_name");//配偶工作单位
		map.put("indiv_sps_posl", "decode(indiv_psp_crtfctn,'0','01','1','02','2','03','3','04','9','01')");//职称
		map.put("indiv_sps_duty", "decode(indiv_sps_duty,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//职务
		map.put("indiv_sps_phone", "indiv_sps_mphn");//配偶手机号码
		map.put("indiv_sps_unit_phe", "indiv_sps_phn");//配偶单位电话
		map.put("indiv_sps_annl_income", "indiv_sps_mincm*12");//配偶年收入(元)
		map.put("indiv_sps_job_dt", "indiv_sps_job_dt");//配偶参加工作年份
		map.put("rel_dgr", "decode(com_rel_dgr,'01','00','02','01','03','02')");//与我行合作关系
		map.put("init_loan_date", "com_init_loan_date");//建立信贷关系时间
		map.put("hold_card", "hold_card");//持卡情况，按老码值进行替换，不需转换
		map.put("passport_flg", "decode(passport_flg,'1','Y','2','N','')");//是否有国外护照
		map.put("crd_grade", "decode(crd_grade,'00','11','11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','27','12','37','13','')");//信用等级
		map.put("crd_date", "crd_date");//信用评定日期
		map.put("remark", "remark");//备注
//		map.put("cus_ccr_model_id", "");//评级模型，老系统无此字段，不移植
//		map.put("cus_ccr_model_name", "");//评级模型名称，老系统无此字段，不移植	
		
		/*
		 * 个人客户-联系信息标签
		 */
		//**特殊处理
		/*
		map.put("post_province", "");//五联通讯地址
		map.put("post_city", "");//五联通讯地址		
		map.put("post_town", "");//五联通讯地址
		map.put("post_country", "");//五联通讯地址
		*/
		map.put("post_area", "area_code");//五联通讯地址
		map.put("post_addr", "post_addr");//通讯地址-详细地址
		map.put("post_code", "post_code");//通讯地址-邮政编码
		
		//**
		/*
		map.put("rsd_province", "");//五联居住地址
		map.put("rsd_city", "");//五联居住地址
		map.put("rsd_area", "");//五联居住地址
		map.put("rsd_town", "");//五联居住地址
		map.put("rsd_country", "");//五联居住地址
		*/
		map.put("indiv_rsd_addr", "indiv_rsd_addr");//居住地址-详细地址
		map.put("indiv_rsd_st", "decode(indiv_rsd_st,'1','10','2','20','3','30','4','40','5','50','6','60','99')");//居住地址-居住状况
		map.put("indiv_zip_code", "indiv_zip_code");//居住地址-居住状况
		map.put("fphone", "fphone");//住宅电话
		map.put("mobile", "mobile");//手机号码
		map.put("phone", "phone");//短信通知号码
		map.put("fax_code", "fax_code");//传真
		map.put("email", "email");//邮箱
		map.put("indiv_occ", "decode(indiv_occ,'0','10000','1','20000','3','30000','4','40000','5','50000','6','60000','7','80100','8','10000','X','70000','Y','80000','Z','80100','80100')");//职业
		map.put("email", "email");//邮箱
		map.put("unit_name", "indiv_com_name");//工作单位
		//**
		map.put("unit_typ", "decode(indiv_com_typ,'100','3400','200','2000','300','9000','400','4000','510','1110','520','1120','530','1160','540','1130','550','1140','560','1150','570','1170','600','1290','610','1310','620','1320','630','1330','640','1340','700','1175','9000')");//单位性质500 未定
		
		map.put("unit_fld", "indiv_com_fld");//单位所属行业编码
		map.put("unit_fld_name", "indiv_com_fld_name");//单位所属行业名称
		/*
		 * 单位五联地址
		map.put("unit_province", "");//单位地址
		map.put("unit_city", "");//单位地址
		map.put("unit_area", "");//单位地址
		map.put("unit_town", "");//单位地址
		map.put("unit_country", "");//单位地址
		*/
		
		map.put("unit_zip_code", "indiv_com_zip_code");//单位邮编
		map.put("unit_phn", "indiv_com_phn");//单位电话
		map.put("unit_fax", "indiv_com_fax");//单位传真
		map.put("unit_cnt_name", "indiv_com_cnt_name");//单位联系人
		map.put("indiv_work_job_y", "indiv_work_job_y");//单位工作起始年
		map.put("job_ttl", "decode(indiv_com_job_ttl,'1','01','2','02','3','03','5','01','6','01','7','02','04')");//职务
		map.put("indiv_crtfctn", "indiv_crtfctn");//职称
		map.put("indiv_ann_incm", "indiv_ann_incm*12");//年收入(元)
		//**
//		map.put("indiv_ann_incm_src", "");//收入来源
		map.put("indiv_sal_acc_bank", "indiv_sal_acc_bank");//工资账户开户行
		map.put("indiv_sal_acc_no", "indiv_sal_acc_no");//工资账号
		map.put("work_resume", "work_resume");//个人简历
		map.put("cus_status", "decode(cus_status,'00','00','20','20','01','01','02','00','03','20')");//客户状态
		//**
//		map.put("main_br_id", "SELECT a.usr_bch FROM s_usr a WHERE a.usr_cde=''");//主管机构
		
		map.put("cust_mgr", "decode(cust_mgr,'',input_id,cust_mgr)");//cust_mgr主管客户经理decode(cust_mgr,'',input_id,cust_mgr)
		map.put("input_id", "input_id");//登记人
		map.put("input_br_id", "input_br_id");//登记机构
		map.put("input_date", "input_date");//登记日期
		map.put("last_chg_usr", "last_upd_id");//更新人
		map.put("last_chg_dt", "last_upd_date");//更新日期
		map.put("instu_cde", "'0000'");//法人编码
		
		map.put("shahd_ind", "decode(is_bank_stk,'1','Y','2','N','')");//是否股东
		
		//**隐藏
		map.put("shares_org", "''");//入股机构，隐藏
		map.put("shares_dt", "''");//入股时间，隐藏
		map.put("unit_fld2", "''");//所属行业2，隐藏
		map.put("unit_fld_name2", "''");//所属行业2名称，隐藏
		map.put("faml_regs_no", "''");//户口簿号，隐藏
		map.put("indiv_oth_alle_ind", "''");//个人其他扶贫标志，隐藏
		map.put("alle_bunb", "''");//扶贫带动人数，隐藏
		map.put("large_agri_iind", "''");//农业专业大户标志，隐藏
		map.put("fami_farm_ind", "''");//家庭农场标志，隐藏
		map.put("idval_type", "''");//个体经营户类型，隐藏
		map.put("idval_type", "''");//个体经营户类型，隐藏
		map.put("inclusive_finance_statistics", "''");//普惠金融统计口径，隐藏
		map.put("is_low_basic", "''");//是否低保户，隐藏
		map.put("is_low_basic", "''");//是否低保户，隐藏
		
		
		
		map.put("shares_amt", "com_hold_stk_amt");//入股金额（元）
		map.put("undertaking_guarant", "under_guar_cus_type");//创业担保贷12类人
		
		//**
		map.put("nat_eco_sec", "''");//国民经济部门
		map.put("unit_ind", "decode(indiv_scom_name,'','N','Y')");//是否有工作单位
		
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
			//处理主管客户经理不存在的数据
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
			
			//更新行政区划-市
			PreparedStatement up_reg_city_ps=null;
			String reg_city_ps=" UPDATE cus_indiv a SET a.post_city=(SELECT b.area_parent_code FROM s_area b WHERE b.area_code=a.post_area) ";
			up_reg_city_ps=conn.prepareStatement(reg_city_ps);
			up_reg_city_ps.execute();
			if(up_reg_city_ps!=null){
				up_reg_city_ps.close();
			}
			
			//更新行政区划-省
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
	
	//导入个人客户关系表cus_loan_rel 
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("PK_ID", "cus_id");//客户号
		map.put("cus_id", "cus_id");//客户号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_type", "decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250','120','111','130','113','110','114','')");//客户类型
//		map.put("cus_short_name", "cus_short_name");//客户简称
//		map.put("cus_en_name", "cus_en_name");//外文名称
		map.put("cert_type", "decode(cert_type,'10','10100','17','10200','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','')");//证件类型
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
