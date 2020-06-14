package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class BuLuWeituo {

	
	public static void main(String[] arg){
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection conn = null;
			conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
			buluCont(conn);
			buluAcc(conn);
			buluWeiDai(conn);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static void buluCont(Connection conn) throws SQLException{
		
		
		Map<String,String>map=new HashMap<String, String>();
//		map.put("serno", "htwd8808820150001");//申请调查编
//		map.put("cont_no", "htwd8808820150001");//合同编号
//		map.put("appl_amt", "20000000");//合同金额
//		map.put("biz_type", "'010881'");//产品编号
//		map.put("prd_name", "'对公委托贷款'");//产品编号
//		map.put("cont_type", "'3'");//合同类型最高发生额
//		
//		
////		map.put("cust_name", "'邢台市一二三四五程有限公司 '");//客户名称
//		map.put("cust_no", "'c011428981'");//客户编号
//		map.put("pvp_type", "'CMIS'");//出账方式
//		map.put("loan_form", "'1'");//发生类型（贷款形式）
//		map.put("limit_ind", "'1'");//用信方式
//		map.put("limit_ma_no", "''");//授信协议编号
//		map.put("limit_acc_no", "''");//额度台帐编号
//		map.put("currency_type", "'CNY'");//贷款币种
//		map.put("exchange_rate", "1");//汇率
//		map.put("term_time_type", "'02'");//期限时间类型
//		map.put("loan_term", "6");//贷款期限
//		map.put("loan_start_date", "'2015-04-28'");//合同起始日
//		map.put("loan_end_date", "'2015-10-28'");//合同到期日
//		
//		
//		map.put("prime_rt_knd", "'2'");//利率类型
//		map.put("ruling_rat_y", "0.0851265");//基准利率(年)
//		map.put("rate_exe_model", "'11'");//利率类型**
//		map.put("discont_ind", "'2'");//是否贴息
//		map.put("reality_ir_y", "0.144");//执行年利率
//		map.put("cal_floating_rate", "1.6916");//浮动比例
//		map.put("flt_ovr_rt", "0.5");//逾期加罚比例
//		map.put("overdue_ir", "0.018");//逾期罚息利率(月）
//		map.put("flt_dfl_rt", "1");//违约加罚比例
//		map.put("default_ir", "0.024");//违约罚息利率(月)
//
//		map.put("rate_exe_model", "'11'");//利率调整方式
//		
//		map.put("assure_means_main", "'00'");//主担保方式
//		map.put("assure_means2", "''");//担保方式1
//		map.put("assure_means3", "''");//担保方式2
//		map.put("loan_direction", "''");//贷款投向代码
//		map.put("direction_name", "''");//贷款投向名称
//		map.put("agriculture_type", "''");//涉农类型,码值一致，不许转换
//		map.put("agriculture_use", "''");//涉农用途
//		map.put("use_dec", "''");//用途描述
//		
//		map.put("repayment_mode", "'102'");//还款方式
//		map.put("interest_acc_mode", "'03'");//按月还息
//		
//		map.put("cust_mgr", "'1359'");//经办人
//		map.put("input_br_id", "'88088'");//经办机构
//		map.put("sign_date", "'20150428'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
//
//		map.put("input_id", "'1359'");//录入人
//		map.put("main_br_id", "'88088'");//主管机构
//		
//		map.put("cont_state", "'200'");//协议状态
//		
		
		//**************
		//*************
		//*************
		//*************
		//*************
		//*************
		//*************
		map=new HashMap<String, String>();
		map.put("iqp_cde", "'htwd8808820150001'");//申请调查编
		map.put("serno", "'htwd8808820150001'");//业务流水号
		map.put("cont_no", "'htwd8808820150001'");//合同编号
		map.put("prd_cde", "'01022205'");//产品编号,单独处理循环产品和不循环产品
		map.put("cont_typ", "'03'");//合同类型，循环合同，一般合同
		
		map.put("cus_name", "'邢台市业峥建筑工程有限公司'");//客户名称
		map.put("cus_id", "'c011428981'");//客户编号
        map.put("charge_stamp_duty", "'N'");//是否收取印花税
		
		map.put("appl_ccy", "'CNY'");//贷款币种
		map.put("appl_amt", "20000000");//贷款金额（元)
		map.put("cont_str_dt", "'2015-04-28'");//合同起始日
		map.put("cont_end_dt", "'2015-10-28'");//合同到期日
		map.put("dc_no", "'htwd8808820150001'");//批复编号
		map.put("Term_Time_Typ", "'03'");//期限时间类型
		map.put("appl_tnr", "6");//贷款期限
		
		map.put("ruling_rat_y", "0.0851265");//基准利率
		map.put("prime_rt_knd", "'2'");//利率类型
		map.put("disc_ind", "'N'");//是否贴息
		map.put("ir_exe_typ", "'1'");//利率执行方式,全部按合同利率
		map.put("floating_typ", "'01'");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "0.144");//固定利率,执行利率
		map.put("reality_rat_y", "0.144");//执行利率(年)
		map.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("overdue_rat", "0.5");//逾期加罚比例
		map.put("overdue_rat_m", "0.216");//逾期罚息利率(年）
		map.put("default_rat", "1");//违约加罚比例
		map.put("default_rat_m", "0.288");//违约罚息利率(年)
		
		map.put("rat_adjust_mode", "'4'");//利率调整方式,如为浮动利率，只能是一月一日
		
		map.put("gurt_typ", "'40'");//主担保方式
//		map.put("loan_direction", "''");//贷款投向代码
//		map.put("direction_name", "direction_name");//贷款投向名称
//		map.put("agriculture_typ", "agriculture_type");//涉农类型,码值一致，不许转换
//		map.put("agriculture_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
//		map.put("use_dec", "use_dec");//用途描述
		map.put("principal_loan_typ", "'1'");
		
		
		map.put("opr_usr", "'1359'");//经办人
		map.put("opr_bch", "'88088'");//经办机构
		map.put("opr_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "'1359'");//协办人
		map.put("coopr_bch", "'88088'");//协办机构
		map.put("coopr_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "'1359'");//登记人
		map.put("crt_bch", "'88088'");//登记机构
		map.put("crt_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "'1359'");//主管人
		map.put("main_bch", "'88088'");//主管机构
		map.put("iqp_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		map.put("cont_sts", "'10'");//协议状态
		map.put("wf_appr_sts", "'997'");//审批状态，全部为通过
		
		map.put("instu_cde", "'0000'");//法人编码
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		PreparedStatement de_ps_cus_corp=null;
		String de_cus_corp=" delete from ctr_corp_loan_entr where cont_no='htwd8808820150001' ";
		de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
		de_ps_cus_corp.execute();
		
		PreparedStatement insert_cus_corp_ps=null;
		String insert_cus_corp=" INSERT INTO ctr_corp_loan_entr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM dual)";
		System.out.println(insert_cus_corp);
		insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
		insert_cus_corp_ps.execute();
		
		//更新个人证件类型、证件号码
		PreparedStatement up_cert_typ_01_ps=null;
		String up_cert_typ_01=" UPDATE ctr_corp_loan_entr a SET cus_name = (SELECT cus_name FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cont_no='htwd8808820150001'";
		up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
		up_cert_typ_01_ps.execute();
		
		//更新对公证件类型、证件号码
		PreparedStatement up_cert_typ_ps=null;
		String up_cert_typ=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cont_no='htwd8808820150001' ";
		up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
		up_cert_typ_ps.execute();
		
		PreparedStatement up_prd_name_ps=null;
		String up_prd_name=" UPDATE ctr_corp_loan_entr a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) where cont_no='htwd8808820150001'";
		up_prd_name_ps=conn.prepareStatement(up_prd_name);
		up_prd_name_ps.execute();
	}
	
	public static void buluAcc(Connection conn) throws SQLException{
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		Map<String,String> map=new HashMap<String, String>();
		map.put("account_status", "'2'");//台账状态,根据核算状态，修改销户核销的状态
		map.put("cont_no", "'htwd8808820150001'");//合同编号
		map.put("bill_no", "'htwd8808820150001-1'");//借据编号
		map.put("cus_name", "'邢台市业峥建筑工程有限公司'");//客户名称
		map.put("cus_id", "'c011428981'");//客户编号
		
		map.put("base_rt_knd", "'10'");//基准利率类型
		
		map.put("prd_cde", "'01022205'");
		map.put("cur_type", "'CNY'");//贷款币种
		map.put("loan_amount", "20000000");//贷款金额（元)
		map.put("loan_start_date", "'2015-04-28'");//贷款发放日
		map.put("loan_end_date", "'2015-10-28'");//贷款终止日
		
		map.put("ruling_ir", "0.0851265");//基准利率
		map.put("ir_flt_typ", "'01'");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "0.144");//固定利率,执行利率
		map.put("floating_pct", "1.6916");//浮动比例
		map.put("rel_ir_y", "0.144");//执行利率(年)
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("ovr_ir_y", "0.216");//逾期罚息利率(年）
		map.put("flt_ovr_rt", "0.5");//逾期加罚比例
		map.put("flt_dfl_rt", "1");//违约加罚比例
		map.put("dfl_ir_y", "0.288");//违约罚息利率(年)
		map.put("assure_means_main", "'40'");//主担保方式
		map.put("repayment_mode", "'CL15'");//还款方式
		map.put("loan_balance", "12000000");//贷款余额(元)
		map.put("revolving_times", "''");//借新还旧次数
		map.put("extension_times", "''");//展期次数
		map.put("cla", "''");//五级分类
		
		map.put("prime_rt_knd", "'2'");//利率类型
		map.put("disc_ind", "'N'");//是否贴息
		map.put("next_repc_opt", "'4'");//利率调整方式,如为浮动利率，只能是一月一日
		map.put("dirt_cde", "''");//贷款投向代码
		map.put("dirt_name", "''");//贷款投向名称
		map.put("agr_typ", "''");//涉农类型,码值一致，不许转换
		map.put("serno", "'htwd8808820150001'");//业务流水号
		map.put("cont_typ", "'03'");//合同类型，循环合同，一般合同
		map.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map.put("usg_dsc", "''");//用途描述
		
		map.put("opr_usr", "'1359'");//经办人
		map.put("opr_bch", "'88088'");//经办机构
		map.put("opr_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "'1359'");//协办人
		map.put("coopr_bch", "'88088'");//协办机构
		map.put("coopr_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "'1359'");//登记人
		map.put("crt_bch", "'88088'");//登记机构
		map.put("crt_dt", "'2015-04-28'");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("cus_manager", "'1359'");//主管人
		map.put("main_br_id", "'88088'");//主管机构

		
		map.put("instu_cde", "'0000'");//法人编码
		
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
			String de_cus_corp=" delete from Acc_Corp_Loan a where a.bill_no='htwd8808820150001-1' ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Acc_Corp_Loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM dual)";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//更新个人证件类型、证件号码
			PreparedStatement up_cert_typ_02_ps=null;
			String up_cert_typ_02=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cont_no='htwd8808820150001' ";
			up_cert_typ_02_ps=conn.prepareStatement(up_cert_typ_02);
			up_cert_typ_02_ps.execute();
			
			
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Acc_Corp_Loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) where cont_no='htwd8808820150001' ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
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
	
	public static void buluWeiDai(Connection conn) throws SQLException{

		PreparedStatement up1_ps=null;
		String up1=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p008669724','李如海','10100','130504196605171515',0.002,20000,'6212385001000053767','无' FROM dual) WHERE a.cont_no='htwd8808820140001' ";
		up1_ps=conn.prepareStatement(up1);
		up1_ps.execute();
		if(up1_ps!=null){
			up1_ps.close();
		}
		
		PreparedStatement up2_ps=null;
		String up2=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p000101356','朱金梅','10100','130504196312191520',0.002,20000,'6212385001000110344','无' FROM dual) WHERE a.cont_no='htwd8808820140007' ";
		up2_ps=conn.prepareStatement(up2);
		up2_ps.execute();
		if(up2_ps!=null){
			up2_ps.close();
		}
		
		PreparedStatement up3_ps=null;
		String up3=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p008669724','李如海','10100','130504196605171515',0.002,40000,'6212385001000053767','无' FROM dual) WHERE a.cont_no='htwd8808820150001' ";
		up3_ps=conn.prepareStatement(up3);
		up3_ps.execute();
		if(up3_ps!=null){
			up3_ps.close();
		}
		
		PreparedStatement up4_ps=null;
		String up4=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','1','c012325228','邢台惠农农业技术开发有限公司','20100','05546998-1',0.002,40000,'8801812002000002813','无' FROM dual) WHERE a.cont_no='营业部委贷2014001号' ";
		up4_ps=conn.prepareStatement(up4);
		up4_ps.execute();
		if(up4_ps!=null){
			up4_ps.close();
		}
	}
	
}
