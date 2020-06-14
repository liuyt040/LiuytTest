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
 * 贷款合同、委托贷款、保理）
 */
public class CtrLoanCont2CtrLoanCont {

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
			/*test2(conn);
			test3(conn);
			test4(conn);
			testCtrRpy(conn);*/
//			if(first(conn)){
//				test1(conn);
//				test2(conn);
//				test3(conn);
//				test4(conn);
//				testCtrRpy(conn);
//			}else{
//				System.out.println("老系统存在合同已签订，但未放款的合同，需要全部做注销处理后才能移植，否则会造成授信出错");
//			}
			
			
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
		String count=" SELECT cust_no,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_loan_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_loan@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
		try {
			count_ps=conn.prepareStatement(count);
			ResultSet rs =count_ps.executeQuery();
			while(rs.next()){
				cont_num=false;
				System.out.println("合同号："+rs.getString("CONT_NO")+" 授信台账："+rs.getString("LIMIT_ACC_NO")+" 授信协议："+rs.getString("LIMIT_MA_NO"));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		return cont_num;
		
	}

	//导入贷款合同主表,不包含委托和保理，不再一张表
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//申请调查编
		map.put("serno", "serno");//业务流水号
		map.put("cont_no", "cont_no");//合同编号
//		map.put("cont_name", "cont_no");//中文合同编号
		map.put("gra_avail_amt", "cont_amt");//批单可用金额
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//利率类型
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
		map.put("other_area_loan", "'010'");//是否异地贷款
		
		//** 对公和个人不一样，需要特殊处理，产品编码需要映射
		/*
		map.put("payment_mode", "''");//支付方式，根据核心数据处理
		map.put("grp_name", "guarantora_grp_name");//联保小组名称
		*/
		map.put("prd_cde", "decode(biz_type,'000015','01029340','010169','01021315','010006','01027580','000027','01026960','000058','01026960','000058','01026960','010273','01026960','010881','01022205','000028','01025910','010374','01026960','010171','01022100','010172','01022100','010173','01022100','000121','01011040','021180','01018946','021060','01018319','000004','01012974','000005','01015330','000017','01012606','000052','01017030','005040','01014350','010005','01011522','010005','01011522','022179','01012731','000006','01017162','000018','01011864','000019','01014244','000020','01013896','000051','01018133','010004','01014172','010007','01014921','010008','01011589','010009','01014730','021061','01017856','022178','01017162','022184','01017863','000091','01023134',biz_type)");//产品编号,单独处理循环产品和不循环产品
		map.put("cont_typ", "decode(cont_type,'2','02','03')");//合同类型，循环合同，一般合同
		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//是否循环
		
		map.put("cus_name", "cust_name");//客户名称
		map.put("cus_id", "cust_no");//客户编号
		map.put("cus_typ", "decode(substr(cust_no,1,1),'p','114','c','211')");//客户
//		map.put("rbp_ind", "");//是否随借随还
//		map.put("stn_prd_ind", "");//是否标准产品
//		map.put("lr_ind", "");//是否低风险产品
		map.put("charge_stamp_duty", "'N'");//是否收取印花税
		map.put("loan_type", "decode(pvp_type,'MBSP','MBSP','ZHSP')");//出账方式
		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//发生类型（贷款形式）
		map.put("limit_ind", "limit_ind");//用信方式
		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("appl_ccy", "currency_type");//贷款币种
		map.put("appl_amt", "cont_amt");//贷款金额（元)
		map.put("exc_rt", "exchange_rate");//汇率
		map.put("tnm_time_typ", "decode(term_time_type,'01','01','02','03','03','06')");//期限时间类型
		map.put("appl_tnr", "loan_term");//贷款期限
		map.put("cont_str_dt", "loan_start_date");//合同起始日
		map.put("cont_end_dt", "loan_end_date");//合同到期日
		
		map.put("dc_no", "serno");//批复编号
		
		map.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基准利率
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map.put("ir_exe_typ", "'1'");//利率执行方式,全部按合同利率
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
//		map.put("fix_int_sprd", "cont_amt");//固定利差
		map.put("flt_rt_pct", "cal_floating_rate");//浮动比例
		map.put("rel_ir_y", "reality_ir_y");//执行利率(年)
//		map.put("rel_ir_m", "loan_start_date");//执行利率(月),无此字段
		map.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("flt_ovr_rt", "overdue_rate");//逾期加罚比例
		map.put("flt_dfl_rt", "default_rate");//违约加罚比例
		
//		map.put("fix_ovr_sprd", "loan_start_date");//逾期加罚利差(没用)
//		map.put("ovr_ir_y1", "exchange_rate");//逾期罚息利率(没用)
//		map.put("fix_dfl_sprd", "loan_start_date");//违约加罚利差(没用)
//		map.put("dfl_ir_y1", "loan_start_date");//违约罚息利率(没用)
		map.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("dirt_cde", "loan_direction");//贷款投向代码
		map.put("dirt_name", "direction_name");//贷款投向名称
		map.put("agr_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
		map.put("usg_dsc", "use_dec");//用途描述
		
		
		map.put("grn_ind", "'N'");//是否绿色信贷
//		map.put("gurt_grp_cont_no", "''");//联保协议编号
		map.put("is_cop", "'N'");//是否占用合作方额度
		
		map.put("BIZ_MDL_ID", "'CC'");//产品模式-综合授信
		
		map.put("opr_usr", "cust_mgr");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cust_mgr");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cust_mgr");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cust_mgr");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//协议状态
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_loan_cont a  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_loan_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type not in ('010881','022184','010882') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新逾期罚息利率,违约罚息利率
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE ctr_loan_cont a SET a.ovr_ir_y=a.rel_ir_y*1.5,a.dfl_ir_y=a.rel_ir_y*2  ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			//更新是否低风险，对公使用授信为高风险，其他为低风险
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE ctr_loan_cont a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no )  ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//更新是否低风险，个人贷款根据是否低风险字段判断
			PreparedStatement up_low_risk=null;
			String low_risk=" UPDATE ctr_loan_cont a SET a.is_low_risk = ( SELECT decode(b.low_risk_flg,'1','Y','N') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'p%' AND b.cont_no=a.cont_no )";
			up_low_risk=conn.prepareStatement(low_risk);
			up_low_risk.execute();
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE ctr_loan_cont a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde )  ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_loan_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//更新个人证件类型、证件号码
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE ctr_loan_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) where a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			//将循环类的流动资金贷款产品号更新为01022512（流动资金循环贷款）
			PreparedStatement up_prd_liudong_ps=null;
			String up_prd_liudong=" UPDATE ctr_loan_cont a SET a.prd_cde='01022512',a.prd_name='流动资金循环贷款' WHERE a.prd_cde='01021315' AND a.cont_typ='02' ";
			up_prd_liudong_ps=conn.prepareStatement(up_prd_liudong);
			up_prd_liudong_ps.execute();
			
			
			//跟新合同中的客户类型
			PreparedStatement up_cus_typ_ps=null;
			String up_cus_typ=" UPDATE ctr_loan_cont a SET a.cus_typ = ( SELECT cus_type FROM (SELECT b.cus_type,cus_id FROM cus_corp b UNION ALL SELECT c.cus_type,cus_id FROM cus_indiv  c ) d WHERE d.cus_id=a.cus_id)  ";
			up_cus_typ_ps=conn.prepareStatement(up_cus_typ);
			up_cus_typ_ps.execute();
			if(up_cus_typ_ps!=null){
				up_cus_typ_ps.close();
			}
			
			
			//跟新合同中的支付类型-自主支付,如果有任何一笔台账为自主支付，则合同为自主支付
			PreparedStatement up_payment_mode_01_ps=null;
			String up_payment_mode_01=" UPDATE ctr_loan_cont a SET a.payment_mode ='01' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='1') ";
			up_payment_mode_01_ps=conn.prepareStatement(up_payment_mode_01);
			up_payment_mode_01_ps.execute();
			if(up_payment_mode_01_ps!=null){
				up_payment_mode_01_ps.close();
			}
			
			//跟新合同中的支付类型-受托支付，如果有任何一笔台账为受托支付，则合同改为受托支付，由于受托支付后执行，所以既有同一笔合同既有自主支付，又有受托支付台账的会变为受托支付
			PreparedStatement up_payment_mode_02_ps=null;
			String up_payment_mode_02=" UPDATE ctr_loan_cont a SET a.payment_mode ='02' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='2') ";
			up_payment_mode_02_ps=conn.prepareStatement(up_payment_mode_02);
			up_payment_mode_02_ps.execute();
			if(up_payment_mode_02_ps!=null){
				up_payment_mode_02_ps.close();
			}
			
			//8856820160031 占用的授信台账编号应由【2016EDTZ88568000004】改为【2016EDTZ88568000002】
			PreparedStatement up_item_id_ps=null;
			String up_item_id=" UPDATE ctr_loan_cont a SET a.lmt_acc_no='2016EDTZ88568000002' WHERE a.cont_no='8856820160031' ";
			up_item_id_ps=conn.prepareStatement(up_item_id);
			up_item_id_ps.execute();
			if(up_item_id_ps!=null){
				up_item_id_ps.close();
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
			if(up_low_risk!=null){
				up_low_risk.close();
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
			if(up_prd_liudong_ps!=null){
				up_prd_liudong_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//导入委托贷款合同主表
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//申请调查编
		map.put("serno", "serno");//业务流水号
		map.put("cont_no", "cont_no");//合同编号
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//利率类型
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
//		map.put("cont_name", "cont_no");//中文合同编号
//		map.put("gra_avail_amt", "cont_amt");//批单可用金额
//		map.put("other_area_loan", "'1'");//是否异地贷款		
		map.put("prd_cde", "decode(biz_type,'022184','01017863','010881','01022205')");//产品编号,单独处理循环产品和不循环产品
		map.put("cont_typ", "decode(cont_type,'2','02','03')");//合同类型，循环合同，一般合同
//		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//是否循环
		
		map.put("cus_name", "cust_name");//客户名称
		map.put("cus_id", "cust_no");//客户编号
//		map.put("rbp_ind", "");//是否随借随还
//		map.put("stn_prd_ind", "");//是否标准产品
//		map.put("lr_ind", "");//是否低风险产品
        map.put("charge_stamp_duty", "'N'");//是否收取印花税
		
//		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//发生类型（贷款形式）
//		map.put("limit_ind", "limit_ind");//用信方式
//		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
//		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("appl_ccy", "currency_type");//贷款币种
		map.put("appl_amt", "cont_amt");//贷款金额（元)
//		map.put("exc_rt", "exchange_rate");//汇率
		map.put("cont_str_dt", "loan_start_date");//合同起始日
		map.put("cont_end_dt", "loan_end_date");//合同到期日
		map.put("dc_no", "serno");//批复编号
		map.put("Term_Time_Typ", "decode(term_time_type,'01','01','02','03','03','06')");//期限时间类型
		map.put("appl_tnr", "loan_term");//贷款期限
		
		map.put("ruling_rat_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基准利率
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map.put("ir_exe_typ", "'1'");//利率执行方式,全部按合同利率
		map.put("floating_typ", "'01'");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
//		map.put("fix_int_sprd", "cont_amt");//固定利差
		map.put("floating_pct", "cal_floating_rate");//浮动比例
		map.put("reality_rat_y", "reality_ir_y");//执行利率(年)
//		map.put("rel_ir_m", "loan_start_date");//执行利率(月),无此字段
		map.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("overdue_rat", "overdue_rate");//逾期加罚比例
		map.put("overdue_rat_m", "overdue_ir*12");//逾期罚息利率(年）
		map.put("default_rat", "default_rate");//违约加罚比例
		map.put("default_rat_m", "default_ir*12");//违约罚息利率(年)
		
//		map.put("fix_ovr_sprd", "loan_start_date");//逾期加罚利差(没用)
//		map.put("ovr_ir_y1", "exchange_rate");//逾期罚息利率(没用)
//		map.put("fix_dfl_sprd", "loan_start_date");//违约加罚利差(没用)
//		map.put("dfl_ir_y1", "loan_start_date");//违约罚息利率(没用)
		map.put("rat_adjust_mode", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("loan_direction", "loan_direction");//贷款投向代码
		map.put("direction_name", "direction_name");//贷款投向名称
		map.put("agriculture_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map.put("agriculture_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
		map.put("use_dec", "use_dec");//用途描述
		map.put("principal_loan_typ", "decode(loan_form,'1','1','3','2','1')");
		
//		map.put("gurt_grp_cont_no", "''");//联保协议编号
//		map.put("is_cop", "'N'");//是否占用合作方额度
		
//		map.put("BIZ_MDL_ID", "'CC'");//产品模式-综合授信
		
		map.put("opr_usr", "cust_mgr");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cust_mgr");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cust_mgr");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cust_mgr");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//协议状态
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_corp_loan_entr a   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_corp_loan_entr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type  in ('010881','022184') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			//更新逾期罚息利率,违约罚息利率
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE ctr_corp_loan_entr a SET a.overdue_rat_m=a.reality_rat_y*1.5,a.default_rat_m=a.reality_rat_y*2 ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE ctr_corp_loan_entr a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//更新个人证件类型、证件号码
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) where a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_low_risk!=null){
				up_low_risk.close();
			}*/
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	/*
	 * 委托贷款从表
	 */
	public static void test4(Connection conn) {
		Map<String,String> map3 = new HashMap<String,String>();
		map3.put("principal_typ", "decode(principal_type,'04','5','03','3','01','241','1')");
//		map3.put("principal_loan_typ", "decode(loan_form,'1','1','3','2','1')");
		map3.put("principal_cus_id", "principal_cus_id");
		map3.put("principal_name", "principal_name");
		map3.put("principa_cert_typ", "decode(principa_cert_type,'10','10100','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','29900')");
		map3.put("principa_cert_cde", "principa_cert_code");
		map3.put("comm_charge_rate", "comm_charge_rate");
		map3.put("comm_charge_amt", "comm_charge_amt");
		map3.put("principal_bal_acct", "principal_bal_acct");
		map3.put("entrust_cond", "entrust_cond");
		
		StringBuffer keys3=new StringBuffer("");
		StringBuffer values3=new StringBuffer("");
		for (Map.Entry<String, String> entry3 : map3.entrySet()) { 
			keys3.append(","+entry3.getKey());
			values3.append(","+entry3.getValue());
		}
		
		PreparedStatement principal_ps=null;
		String principal=" UPDATE ctr_corp_loan_entr a SET ("+keys3.toString().substring(1)+") = ( SELECT "+values3.toString().substring(1)+" FROM Ctr_Com_Loan_Entr@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) where a.cont_no in ( select cont_no from ctr_loan_cont@CMISTEST2101) ";
		System.out.println(principal);
		try {
			principal_ps=conn.prepareStatement(principal);
			principal_ps.execute();
			if(principal_ps!=null){
				principal_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//导入保理合同主表
	public static void test3(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//申请调查编
		map.put("serno", "serno");//业务流水号
		map.put("cont_no", "cont_no");//合同编号
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//利率类型
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR利率类型
		map.put("fix_int_sprd", "lpr_natu_base_point");//浮动基点数
		map.put("gra_avail_amt", "cont_amt");//批单可用金额
//		map.put("other_area_loan", "'1'");//是否异地贷款
		
		//** 对公和个人不一样，需要特殊处理，产品编码需要映射
		/*
		map.put("payment_mode", "''");//支付方式，根据核心数据处理
		map.put("grp_name", "guarantora_grp_name");//联保小组名称
		*/
		map.put("prd_cde", "'01029525'");//产品编号
		map.put("cont_typ", "decode(cont_type,'2','02','01')");//合同类型，循环合同，一般合同
		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//是否循环
		
		map.put("cus_name", "cust_name");//客户名称
		map.put("cus_id", "cust_no");//客户编号
//		map.put("rbp_ind", "");//是否随借随还
//		map.put("stn_prd_ind", "");//是否标准产品
//		map.put("lr_ind", "");//是否低风险产品
        map.put("charge_stamp_duty", "'N'");//是否收取印花税
		
		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//发生类型（贷款形式）
		map.put("limit_ind", "limit_ind");//用信方式
		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("appl_ccy", "currency_type");//贷款币种
		map.put("appl_amt", "cont_amt");//贷款金额（元)
		map.put("exc_rt", "exchange_rate");//汇率
		map.put("cont_str_dt", "loan_start_date");//合同起始日
		map.put("cont_end_dt", "loan_end_date");//合同到期日
		map.put("tnm_time_typ", "decode(term_time_type,'01','01','02','03','03','06')");//期限时间类型
		map.put("appl_tnr", "loan_term");//贷款期限
		
		map.put("dc_no", "serno");//批复编号
		
		map.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//基准利率
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//利率类型
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//是否贴息
		map.put("ir_exe_typ", "'1'");//利率执行方式,全部按合同利率
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//利率浮动类型,全部按比例浮动
		map.put("fix_int_rt", "reality_ir_y");//固定利率,执行利率
//		map.put("fix_int_sprd", "cont_amt");//固定利差
		map.put("flt_rt_pct", "cal_floating_rate");//浮动比例
		map.put("rel_ir_y", "reality_ir_y");//执行利率(年)
//		map.put("rel_ir_m", "loan_start_date");//执行利率(月),无此字段
		map.put("od_fine_flt_typ", "'01'");//罚息利率浮动类型,全部按执行利率
		map.put("od_cmp_mde", "'1'");//罚息计算方式，全部按比例计算
		map.put("flt_ovr_rt", "overdue_rate");//逾期加罚比例
		map.put("ovr_ir_m", "overdue_ir*12");//逾期罚息利率(年）ovr_ir_m
		map.put("flt_dfl_rt", "default_rate");//违约加罚比例
		map.put("dfl_ir_y", "default_ir*12");//违约罚息利率(年)
		
//		map.put("fix_ovr_sprd", "loan_start_date");//逾期加罚利差(没用)
//		map.put("ovr_ir_y1", "exchange_rate");//逾期罚息利率(没用)
//		map.put("fix_dfl_sprd", "loan_start_date");//违约加罚利差(没用)
//		map.put("dfl_ir_y1", "loan_start_date");//违约罚息利率(没用)
		map.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//利率调整方式,如为浮动利率，只能是一月一日
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("dirt_cde", "loan_direction");//贷款投向代码
		map.put("dirt_name", "direction_name");//贷款投向名称
		map.put("agr_typ", "agriculture_type");//涉农类型,码值一致，不许转换
		map.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//涉农用途
		map.put("use_dec", "use_dec");//用途描述
		
		
//		map.put("grn_ind", "loan_direction");//是否绿色信贷
//		map.put("gurt_grp_cont_no", "''");//联保协议编号
		map.put("is_cop", "'N'");//是否占用合作方额度
		
//		map.put("BIZ_MDL_ID", "'CC'");//产品模式-综合授信
		
		map.put("opr_usr", "cust_mgr");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cust_mgr");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cust_mgr");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cust_mgr");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//协议状态
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from Ctr_Fact_Cont a  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Ctr_Fact_Cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type  = '010882' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新逾期罚息利率,违约罚息利率
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE Ctr_Fact_Cont a SET a.ovr_ir_m=rel_ir_y*1.5,dfl_ir_y=rel_ir_y*2 ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			//跟新合同中的支付类型-自主支付,如果有任何一笔台账为自主支付，则合同为自主支付
			PreparedStatement up_payment_mode_01_ps=null;
			String up_payment_mode_01=" UPDATE Ctr_Fact_Cont a SET a.payment_mode ='01' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='1') ";
			up_payment_mode_01_ps=conn.prepareStatement(up_payment_mode_01);
			up_payment_mode_01_ps.execute();
			if(up_payment_mode_01_ps!=null){
				up_payment_mode_01_ps.close();
			}
			
			//跟新合同中的支付类型-受托支付，如果有任何一笔台账为受托支付，则合同改为受托支付，由于受托支付后执行，所以既有同一笔合同既有自主支付，又有受托支付台账的会变为受托支付
			PreparedStatement up_payment_mode_02_ps=null;
			String up_payment_mode_02=" UPDATE Ctr_Fact_Cont a SET a.payment_mode ='02' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='2') ";
			up_payment_mode_02_ps=conn.prepareStatement(up_payment_mode_02);
			up_payment_mode_02_ps.execute();
			if(up_payment_mode_02_ps!=null){
				up_payment_mode_02_ps.close();
			}
			
			//更新是否低风险，对公使用授信为高风险，其他为低风险
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE Ctr_Fact_Cont a SET a.lr_ind = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE  b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Ctr_Fact_Cont a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde )  ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE Ctr_Fact_Cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
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
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//导入还款方式 
	public static void testCtrRpy(Connection conn) {
		//key:新表字段   
		//value:老表字段
		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_rpy  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" INSERT INTO CTR_RPY (PK_ID, CONT_NO, MTD_CDE, RPY_SRC_DSC, MTD_FREQ_UNIT,mtd_freq,repay_day,repay_date) SELECT 'pk' || CONT_NO,CONT_NO,DECODE(REPAYMENT_MODE,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31'),REPAYMENT_SRC_DEC,DECODE(INTEREST_ACC_MODE,'03','03','04','04','06','03',INTEREST_ACC_MODE),'1',decode(repayment_mode,'201','20','202','20','21'),'10' FROM CTR_LOAN_CONT@CMISTEST2101 ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();

			
			PreparedStatement up_mtd_freq_unit=null;
			String mtd_freq_unit=" UPDATE ctr_rpy a SET mtd_freq_unit = '03',a.mtd_freq='1' WHERE a.mtd_freq_unit='01' ";
			up_mtd_freq_unit=conn.prepareStatement(mtd_freq_unit);
			up_mtd_freq_unit.execute();
			if(up_mtd_freq_unit!=null){
				up_mtd_freq_unit.close();
			}
			
			PreparedStatement up_mtd_freq_unit2=null;
			String mtd_freq_unit2=" UPDATE ctr_rpy a SET mtd_freq_unit = '04',a.mtd_freq='2' WHERE a.mtd_freq_unit='08'  ";//按半年
			up_mtd_freq_unit2=conn.prepareStatement(mtd_freq_unit2);
			up_mtd_freq_unit2.execute();
			if(up_mtd_freq_unit2!=null){
				up_mtd_freq_unit2.close();
			}
			
			PreparedStatement up_mtd_freq_unit4=null;
			String mtd_freq_unit4=" UPDATE ctr_rpy a SET mtd_freq_unit = '01',a.mtd_freq='1' WHERE a.mtd_freq_unit='02'  ";//按日
			up_mtd_freq_unit4=conn.prepareStatement(mtd_freq_unit4);
			up_mtd_freq_unit4.execute();
			if(up_mtd_freq_unit4!=null){
				up_mtd_freq_unit4.close();
			}
			
			PreparedStatement up_mtd_freq_unit3=null;
			String mtd_freq_unit3=" UPDATE ctr_rpy a SET mtd_freq_unit = '02',a.mtd_freq='1' WHERE a.mtd_freq_unit='07'  ";//按周
			up_mtd_freq_unit3=conn.prepareStatement(mtd_freq_unit3);
			up_mtd_freq_unit3.execute();
			if(up_mtd_freq_unit3!=null){
				up_mtd_freq_unit3.close();
			}
			
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
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

