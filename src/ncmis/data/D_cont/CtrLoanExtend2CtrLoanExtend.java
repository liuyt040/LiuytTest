package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * 展期协议 Ctr_Loan_Extend 2 Ctr_Loan_Extend
 */
public class CtrLoanExtend2CtrLoanExtend {

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
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入展期协议
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("cont_no", "cont_no");//合同编号
		map.put("iqp_cde", "serno");//申请编号
		map.put("serno", "serno");//业务流水号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("bill_no", "o_bill_no");//原借据编号
		map.put("o_cont_no", "o_cont_no");//原合同编号
		map.put("prd_cde", "decode(biz_type,'000015','01029340','010169','01021315','010006','01027580','000027','01026960','000058','01026960','000058','01026960','010273','01026960','010881','01022205','000028','01025910','010374','01026960','010171','01022100','010172','01022100','010173','01022100','000121','01011040','021180','01018946','021060','01018319','000004','01012974','000005','01015330','000017','01012606','000052','01017030','005040','01014350','010005','01011522','010005','01011522','022179','01012731','000006','01017162','000018','01011864','000019','01014244','000020','01013896','000051','01018133','010004','01014172','010007','01014921','010008','01011589','010009','01014730','021061','01017856','022178','01017162','022184','01017863',biz_type)");//1一般承兑，2保兑仓
		map.put("appl_ccy", "o_cur_type");//原贷款币种
		map.put("loan_amount", "o_loan_amount");//原贷款金额（元）
		map.put("loan_balance", "o_loan_balance");//原贷款余额（元）
		map.put("loan_start_date", "o_start_date");//原贷款起始日
		map.put("loan_end_date", "o_end_date");//原贷款终止日
		map.put("reality_ir_y", "o_reality_ir_y");//原执行利率(年）
		map.put("overdue_rate", "o_overdue_rt");//原逾期加罚率
		
		map.put("extend_amt", "extend_amt");//展期金额（元）
		map.put("extend_base_ir_y", "ruling_ir");//基准利率
		map.put("extend_flt_typ", "'01'");//利率浮动类型
		map.put("extend_flt_rt_pct", "extend_floating_rt");//浮动比例
		map.put("extend_rel_ir_y", "extend_real_ir_y");//执行利率
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
		
		map.put("extend_reason", "extend_reason");//展期原因
		map.put("extend_oth_dec", "extend_oth_dec");//展期其他情况说
		map.put("extend_fix_int_rt", "extend_real_ir_y");
		map.put("extend_od_fine_flt_typ", "'01'");
		map.put("extend_od_cmp_mde", "'1'");
		map.put("extend_flt_ovr_rt", "0.5");
		map.put("extend_ovr_ir_y", "extend_real_ir_y*1.5");
		map.put("extend_flt_dfl_rt", "1");
		map.put("extend_dfl_ir_y", "extend_real_ir_y*2");
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90','800','10')");//协议状态
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
			String de_cus_corp=" delete from Ctr_Loan_Extend where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Ctr_Loan_Extend ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Ctr_Loan_Extend@CMISTEST2101 b WHERE b.cont_state='800' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE Ctr_Loan_Extend a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			//更新产品名称
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Ctr_Loan_Extend a SET (a.prd_name,prd_type,biz_type_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde )  ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			//更新原基准利率类型
			PreparedStatement up_base_rt_knd_ps=null;
			String up_base_rt_knd=" UPDATE Ctr_Loan_Extend a SET a.base_rt_knd = ( SELECT decode(b.rate_type,'2','30','10') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.o_cont_no )  ";
			up_base_rt_knd_ps=conn.prepareStatement(up_base_rt_knd);
			up_base_rt_knd_ps.execute();
			if(up_base_rt_knd_ps!=null){
				up_base_rt_knd_ps.close();
			}
			
			//更新利率类型
			PreparedStatement up_extend_prime_rt_knd_ps=null;
			String up_extend_prime_rt_knd=" UPDATE Ctr_Loan_Extend a SET a.extend_prime_rt_knd = ( SELECT decode(b.rate_exe_model,'11','2','12','1') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.o_cont_no )  ";
			up_extend_prime_rt_knd_ps=conn.prepareStatement(up_extend_prime_rt_knd);
			up_extend_prime_rt_knd_ps.execute();
			if(up_extend_prime_rt_knd_ps!=null){
				up_extend_prime_rt_knd_ps.close();
			}
			
			//更新主担保方式
			PreparedStatement up_assure_means_main_ps=null;
			String up_assure_means_main=" UPDATE Ctr_Loan_Extend a SET a.assure_means_main = ( SELECT decode(assure_means_main,'00','40',assure_means_main) FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.o_cont_no )  ";
			up_assure_means_main_ps=conn.prepareStatement(up_assure_means_main);
			up_assure_means_main_ps.execute();
			if(up_assure_means_main_ps!=null){
				up_assure_means_main_ps.close();
			}
			//更新原还款方式
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE Ctr_Loan_Extend a SET a.repayment_mode = ( SELECT mtd_cde FROM ctr_rpy b WHERE b.cont_no=a.o_cont_no )  ";
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.execute();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//更新原基准利率(年)
			PreparedStatement up_ruling_ir_ps=null;
			String up_ruling_ir=" UPDATE Ctr_Loan_Extend a SET a.ruling_ir = ( SELECT ruling_ir FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.o_cont_no )  ";
			up_ruling_ir_ps=conn.prepareStatement(up_ruling_ir);
			up_ruling_ir_ps.execute();
			if(up_ruling_ir_ps!=null){
				up_ruling_ir_ps.close();
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
	
}

