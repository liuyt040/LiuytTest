package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccLoan2AccLoan {
	
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
//			test2(conn);
//			test3(conn);
//			test4(conn);
//			test5(conn);
//			test6(conn);
//			test7(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入贷款台账，
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//台账状态,根据核算状态，修改销户核销的状态
		map.put("cont_no", "cont_no");//合同编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("prd_cde", "decode(biz_type,'000015','01029340','010169','01021315','010006','01027580','000027','01026960','000058','01026960','000058','01026960','010273','01026960','010881','01022205','000028','01025910','010374','01026960','010171','01022100','010172','01022100','010173','01022100','000121','01011040','021180','01018946','021060','01018319','000004','01012974','000005','01015330','000017','01012606','000052','01017030','005040','01014350','010005','01011522','010005','01011522','022179','01012731','000006','01017162','000018','01011864','000019','01014244','000020','01013896','000051','01018133','010004','01014172','010007','01014921','010008','01011589','010009','01014730','021061','01017856','022178','01017162','022184','01017863','000091','01023134',biz_type)");//产品编号,单独处理循环产品和不循环产品
		map.put("cur_type", "cur_type");//贷款币种
		map.put("loan_amount", "loan_amount");//贷款金额（元)
		map.put("loan_start_date", "loan_start_date");//贷款发放日
		map.put("loan_end_date", "loan_end_date");//贷款终止日
		map.put("loan_end_date_old", "orig_expi_date");//原到期日期
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
		map.put("flt_rt_pc", "floating_rate");//浮动比例
		map.put("reality_ir_y", "reality_ir_y");//执行利率(年)
		map.put("ovr_ir_y", "overdue_ir*12");//逾期罚息利率(年）
		map.put("flt_ovr_rt", "overdue_rate");//逾期加罚比例
		map.put("flt_dfl_rt", "default_rate");//违约加罚比例
		map.put("dfl_ir_y", "default_ir*12");//违约罚息利率(年)
		map.put("assure_means_main", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("assure_means2", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("assure_means3", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//还款方式
		map.put("loan_balance", "loan_balance");//贷款余额(元)
		map.put("revolving_times", "''");//借新还旧次数
		map.put("extension_times", "extension_times");//展期次数
		map.put("cla", "cla");//五级分类
		//取合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		map1.put("dirt_cde", "loan_direction");//贷款投向代码
		map1.put("dirt_name", "direction_name");//贷款投向名称
		map1.put("agr_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map1.put("serno", "serno");//业务流水号
		map1.put("base_rt_knd", "decode(rate_type,'2','30',10)");//利率类型
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map1.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
		map1.put("ruling_ir", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基础利率
		
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("cus_manager", "cus_manager");//主管人
		map.put("main_br_id", "input_br_id");//主管机构
		map.put("iqp_dt", "latest_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
//		map.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//协议状态
//		map.put("wf_appr_sts", "'997'");//审批状态，全部为通过
		
		map.put("instu_cde", "'0000'");//法人编码
		
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
			String de_cus_corp=" truncate table acc_loan   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type not in ('010881','022184','010882') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			//根据合同信息跟新借据信息
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE acc_loan a SET ("+keys1.toString().substring(1)+") = ( SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			
			/*
			//重新计算展期次数和借新还旧次数
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_loan a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM acc_loan@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			*/
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE acc_loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//更新个人证件类型、证件号码
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE acc_loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_loan_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_loan b  )";
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
	
	//导入委托贷款台账，acc_loan 2 Acc_Corp_Loan
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//台账状态,根据核算状态，修改销户核销的状态
		map.put("cont_no", "cont_no");//合同编号
		map.put("bill_no", "bill_no");//合同编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
//		map.put("base_rt_knd", "'10'");//利率类型
		map.put("prd_cde", "decode(biz_type,'022184','01017863','010881','01022205')");
		map.put("cur_type", "cur_type");//贷款币种
		map.put("loan_amount", "loan_amount");//贷款金额（元)
		map.put("loan_start_date", "loan_start_date");//贷款发放日
		map.put("loan_end_date", "loan_end_date");//贷款终止日
		map.put("loan_end_date_old", "orig_expi_date");//原到期日期,
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
		map.put("floating_pct", "floating_rate");//浮动比例
		map.put("rel_ir_y", "reality_ir_y");//执行利率(年)
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("ovr_ir_y", "overdue_ir*12");//逾期罚息利率(年）
		map.put("flt_ovr_rt", "overdue_rate");//逾期加罚比例
		map.put("flt_dfl_rt", "default_rate");//违约加罚比例
		map.put("dfl_ir_y", "default_ir*12");//违约罚息利率(年)
		map.put("assure_means_main", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("assure_means2", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("assure_means3", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//还款方式
		map.put("loan_balance", "loan_balance");//贷款余额(元)
		map.put("revolving_times", "''");//借新还旧次数
		map.put("extension_times", "extension_times");//展期次数
		map.put("cla", "cla");//五级分类
		//取合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		map1.put("dirt_cde", "loan_direction");//贷款投向代码
		map1.put("dirt_name", "direction_name");//贷款投向名称
		map1.put("agr_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map1.put("serno", "serno");//业务流水号
		map1.put("cont_typ", "decode(cont_type,'2','02','01')");//合同类型，循环合同，一般合同
		map1.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map1.put("usg_dsc", "use_dec");//用途描述
		map1.put("base_rt_knd", "decode(rate_type,'1','10','2','30')");//利率类型
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map1.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
		map1.put("ruling_ir", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基准利率
		
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("cus_manager", "cus_manager");//主管人
		map.put("main_br_id", "input_br_id");//主管机构
//		map.put("iqp_dt", "latest_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
//		map.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//协议状态
//		map.put("wf_appr_sts", "'997'");//审批状态，全部为通过
		
		map.put("instu_cde", "'0000'");//法人编码
		
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
			String de_cus_corp=" truncate table Acc_Corp_Loan  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Acc_Corp_Loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type in ('010881','022184') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			//根据合同信息跟新借据信息
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE Acc_Corp_Loan a SET ("+keys1.toString().substring(1)+") = ( SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			
			/*
			//重新计算展期次数和借新还旧次数
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_loan a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM acc_loan@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			*/
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Acc_Corp_Loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//更新个人证件类型、证件号码
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_corp_loan_entr a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM Acc_Corp_Loan b  )";
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
	
	//导入保理台账主表
	public static void test3(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//台账状态,根据核算状态，修改销户核销的状态
		map.put("cont_no", "cont_no");//合同编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
//		map.put("base_rt_knd", "'10'");//利率类型
		map.put("prd_cde", "'01029525'");//产品编号
		map.put("loan_amount", "loan_amount");//贷款金额（元)
		map.put("cur_type", "cur_type");//贷款币种
		map.put("loan_start_date", "loan_start_date");//贷款发放日
		map.put("loan_end_date", "loan_end_date");//贷款终止日
		map.put("loan_end_date_old", "orig_expi_date");//原到期日期
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
		map.put("flt_rt_pct", "floating_rate");//浮动比例
		map.put("rel_ir_y", "reality_ir_y");//执行利率(年)
		map.put("ovr_ir_y", "overdue_ir*12");//逾期罚息利率(年）
		map.put("flt_ovr_rt", "overdue_rate");//逾期加罚比例
		map.put("flt_dfl_rt", "default_rate");//违约加罚比例
		map.put("dfl_ir_y", "default_ir*12");//违约罚息利率(年)
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//还款方式
		map.put("loan_balance", "loan_balance");//贷款余额(元)
		map.put("revolving_times", "''");//借新还旧次数
		map.put("extension_times", "extension_times");//展期次数
		map.put("cla", "cla");//五级分类
		//合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("serno", "serno");//业务流水号
		map1.put("cycle_ind", "decode(cont_type,'2','Y','N')");//是否循环
		map1.put("limit_ind", "limit_ind");//用信方式
		map1.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map1.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		map1.put("serno", "serno");//业务流水号
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map1.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
		map1.put("use_dec", "use_dec");//用途描述
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map1.put("dirt_cde", "loan_direction");//贷款投向代码
		map1.put("dirt_name", "direction_name");//贷款投向名称
		map1.put("agr_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map1.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
		map1.put("use_dec", "use_dec");//用途描述
		map1.put("base_rt_knd", "decode(rate_type,'2','30',10)");//利率类型
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map1.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
		map1.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基准利率
		
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "loan_start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		
		map.put("instu_cde", "'0000'");//法人编码
		
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
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table acc_fact ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_fact ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type  = '010882' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_acc_fact_ps=null;
			String up_acc_fact=" update acc_fact a set ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where  b.cont_no=a.cont_no )";
			System.out.println(up_acc_fact);
			up_acc_fact_ps=conn.prepareStatement(up_acc_fact);
			up_acc_fact_ps.execute();
			
			//更新是否低风险，对公使用授信为高风险，其他为低风险
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_fact a SET a.lr_ind = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE acc_fact a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_fact a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM Ctr_Fact_Cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_fact b  )";
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
	
	//根据核算数据库更新普通贷款账务信息
	public static void test4(Connection conn) {
		//key:新表字段   
		//value:老表字段
		try {
			//根据核算数据库更新账务信息
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//贷款余额
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//应收正常利息
			map2.put("inner_actl_int", "SETL_NORM_INT");//实收正常利息
			map2.put("overdue_actl_int", "ps_setl_int");//当前欠表内利息 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//当前欠表外利息
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//应收罚息
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//实收罚息
			map2.put("delay_int_cumu", "ps_setl_od_int");//欠表内罚息
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//欠表外罚息
			map2.put("inner_int_cumu", "accrued_Int_Nor");//当前计提利息
			map2.put("off_int_cumu", "accrued_Int_Pen");//当前计提罚息
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//逾期天数
			map2.put("od_days", "od_days");//欠息天数
			map2.put("od_prcp_days", "od_prcp_days");//欠本天数
			map2.put("overdue_balance", "od_amt");//逾期本金
			map2.put("over_times_current", "ps_od_num");//当前逾期期数
			map2.put("over_times_total", "ps_od_sum");//累计逾期期数
			map2.put("latest_repay_date", "cur_due_dt");//下一次还款日
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//核算账务状态
			map2.put("is_int", "stop_ind");//是否停息标志
			map2.put("stpint_dt", "stop_date");//停息日期
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//逐笔更新，核算视图过大
			String ycloans = " SELECT BILL_NO FROM acc_loan a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_loan a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				System.out.println(low_risk);
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
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
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			
			//将不存在核算的数据置为结清
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_loan a SET a.account_status='4',a.loan_debt_sts='SETL' WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//根据核算更新借据的还款日 due_day
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_loan a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_loan a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_loan_cont a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no GROUP BY loan_cont_no,loan_rate_mode  ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			//利率类型为浮动利率时，利率调整方式为空
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_loan a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//根据核算更新还款方式为空的数据
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_loan a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//计算逾期天数，逾期天数为欠息天数、欠本天数中的最大值
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_loan a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//根据核算数据库更新委托贷款账务信息
	public static void test5(Connection conn) {
		//key:新表字段   
		//value:老表字段
		try {
			//根据核算数据库更新账务信息
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//贷款余额
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//应收正常利息
			map2.put("inner_actl_int", "SETL_NORM_INT");//实收正常利息
			map2.put("overdue_actl_int", "ps_setl_int");//当前欠表内利息 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//当前欠表外利息
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//应收罚息
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//实收罚息
			map2.put("delay_int_cumu", "ps_setl_od_int");//欠表内罚息
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//欠表外罚息
			map2.put("inner_int_cumu", "accrued_Int_Nor");//当前计提利息
			map2.put("off_int_cumu", "accrued_Int_Pen");//当前计提罚息
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//逾期天数
			map2.put("od_days", "od_days");//欠息天数
			map2.put("od_prcp_days", "od_prcp_days");//欠本天数
			map2.put("overdue_balance", "od_amt");//逾期本金
			map2.put("over_times_current", "ps_od_num");//当前逾期期数
			map2.put("over_times_total", "ps_od_sum");//累计逾期期数
			map2.put("latest_repay_date", "cur_due_dt");//下一次还款日
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//核算账务状态
			map2.put("is_int", "stop_ind");//是否停息标志
			map2.put("stpint_dt", "stop_date");//停息日期
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//逐笔更新，核算视图过大
			String ycloans = " SELECT BILL_NO FROM acc_corp_loan a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_corp_loan a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
				
				
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			//将核算不存在的数据置为结清
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_corp_loan a SET a.account_status='4',a.loan_debt_sts='SETL'  WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//跟新借据的还款日
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_corp_loan a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  ) ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_corp_loan a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_corp_loan_entr a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no  GROUP BY loan_cont_no,loan_rate_mode ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			
			//利率类型为浮动利率时，利率调整方式为空
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_corp_loan a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//根据核算更新还款方式为空的数据
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_corp_loan a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//计算逾期天数，逾期天数为欠息天数、欠本天数中的最大值
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_corp_loan a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}


	//根据核算数据库更新保理账务信息
	public static void test6(Connection conn) {
		//key:新表字段   
		//value:老表字段
		try {
			//根据核算数据库更新账务信息
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//贷款余额
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//应收正常利息
			map2.put("inner_actl_int", "SETL_NORM_INT");//实收正常利息
			map2.put("overdue_actl_int", "ps_setl_int");//当前欠表内利息 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//当前欠表外利息
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//应收罚息
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//实收罚息
			map2.put("delay_int_cumu", "ps_setl_od_int");//欠表内罚息
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//欠表外罚息
			map2.put("inner_int_cumu", "accrued_Int_Nor");//当前计提利息
			map2.put("off_int_cumu", "accrued_Int_Pen");//当前计提罚息
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//逾期天数
			map2.put("od_days", "od_days");//欠息天数
			map2.put("od_prcp_days", "od_prcp_days");//欠本天数
			map2.put("overdue_balance", "od_amt");//逾期本金
			map2.put("over_times_current", "ps_od_num");//当前逾期期数
			map2.put("over_times_total", "ps_od_sum");//累计逾期期数
			map2.put("latest_repay_date", "cur_due_dt");//下一次还款日
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//核算账务状态
			map2.put("is_int", "stop_ind");//是否停息标志
			map2.put("stpint_dt", "stop_date");//停息日期
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//逐笔更新，核算视图过大
			String ycloans = " SELECT BILL_NO FROM acc_fact a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_fact a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
				
				
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			//将核算不存在的数据置为结清
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_fact a SET a.account_status='4',a.loan_debt_sts='SETL'  WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//跟新借据的还款日
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_fact a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_fact a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//根据核算更新利率类型（固定利率，浮动利率）
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_fact_cont a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no GROUP BY loan_cont_no,loan_rate_mode  ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			
			//利率类型为浮动利率时，利率调整方式为空
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_fact a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//根据核算更新还款方式
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_fact a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//计算逾期天数，逾期天数为欠息天数、欠本天数中的最大值
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_fact a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//根据核算数据，更新贷款台账的还款账号字段
	public static void test7(Connection conn) {

		try {
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE acc_loan a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			if(up_acc_loan_ps!=null){
				up_acc_loan_ps.close();
			}
			
			PreparedStatement up_acc_fact_ps=null;
			String up_acc_fact=" UPDATE acc_fact a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_acc_fact_ps=conn.prepareStatement(up_acc_fact);
			up_acc_fact_ps.execute();
			if(up_acc_fact_ps!=null){
				up_acc_fact_ps.close();
			}
			
			PreparedStatement up_Acc_Corp_Loan_ps=null;
			String up_Acc_Corp_Loan=" UPDATE Acc_Corp_Loan a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_Acc_Corp_Loan_ps=conn.prepareStatement(up_Acc_Corp_Loan);
			up_Acc_Corp_Loan_ps.execute();
			if(up_Acc_Corp_Loan_ps!=null){
				up_Acc_Corp_Loan_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
}
