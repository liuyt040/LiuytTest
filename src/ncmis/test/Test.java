package ncmis.test;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class Test {
	private static Map<String,String> map=null;
	public static void main(String... str) throws SQLException{
		String s="SELECT a.guar_no,a.guar_type_cd,a.guar_type_cd_cnname,b.model_id,c.table_name FROM t_guar_base_info a "
			+" LEFT JOIN t_guar_class_info b ON a.guar_type_cd=b.guar_type_cd "
			+" LEFT JOIN T_GUAR_MODEL c ON b.model_id=c.model_id "
			+" WHERE a.guar_no NOT IN ( "
			+" select guar_no from T_INF_CIVIL_AVIATION union all "
			+" select guar_no from T_INF_MACH_EQUI union all "
			+" select guar_no from T_INF_MORTGAGE_OTHER union all "
			+" select guar_no from T_INF_NOBLE_METAL union all "
			+" select guar_no from T_INF_CONTRACT union all "
			+" select guar_no from T_INF_USUF_LAND union all "
			+" select guar_no from T_INF_BOND_PLEDGE union all "
			+" select guar_no from T_INF_STOCK_UNLISTED union all "
			+" select guar_no from T_INF_TRAF_CAR union all "
			+" select guar_no from T_INF_RENTALS_RECE union all "
			+" select guar_no from T_INF_STOCK_LIST union all "
			+" select guar_no from T_INF_REAL_ESTATE union all "
			+" select guar_no from T_INF_BUIL_VEHICLE union all "
			+" select guar_no from T_INF_DEPO_RECEIPT union all "
			+" select guar_no from T_INF_CAR_CER union all "
			+" select guar_no from T_INF_OTHER_FINANCE union all "
			+" select guar_no from T_INF_INCOME union all "
			+" select guar_no from T_INF_SE_CREDIT union all "
			+" select guar_no from T_INF_SWR union all "
			+" select guar_no from T_INF_OTHER_HOUSE union all "
			+" select guar_no from T_INF_CARGO_PLEDGE union all "
			+" select guar_no from T_INF_STOCK_FUND union all "
			+" select guar_no from T_INF_NON_SWR union all "
			+" select guar_no from T_INF_MARINE_BL union all "
			+" select guar_no from T_INF_BUIL_PROJECT union all "
			+" select guar_no from T_INF_BUSINESS_INDUSTRY_HOUSR union all "
			+" select guar_no from T_INF_ACC_RECE union all "
			+" select guar_no from T_INF_ACC_RECE_OTHER union all "
			+" select guar_no from T_INF_PLEDGE_OTHER union all "
			+" select guar_no from T_INF_FINANCE_MANAGE union all "
			+" select guar_no from T_INF_SHIP union all "
			+" select guar_no from T_INF_BILL union all "
			+" select guar_no from T_INF_INSU_SLIP union all "
			+" select guar_no from T_INF_CAR union all "
			+" select guar_no from T_INF_LIVING_ROOM union all "
			+" select guar_no from T_INF_BACKLETTER union all "
			+" select guar_no from T_INF_BUILD_USE )";
		
		System.out.println(s);
	}
	 
	public static void tt(){
//		Map<String, Object> map = new HashMap<String, Object>();
//		String signFile = null;
//		map.put("serno", serno);
//		map.put("cusName", name);
//		map.put("certCode", idNo);
//		map.put("phone", phone);
//		map.put("address", address);
//		map.put("loanStartDate", loanStartDate);
//		map.put("loanEndDate", loanEndDate);
//		map.put("transferAccount", transferAccount);
//		map.put("appLmt", new BigDecimal(appLmt));
//		map.put("contNo", cont_no);
//		
//		MbPrdBaseInfo mbPrdBaseInfo = new MbPrdBaseInfo();
//		mbPrdBaseInfo.setPrdCode(prdCode);
//		mbPrdBaseInfo = (MbPrdBaseInfo) SqlClient.queryAuto(mbPrdBaseInfo, conn);
//		BigDecimal realityIrY = mbPrdBaseInfo.getRealityIrY();
////		Double reality = realityIrY.doubleValue();
//		map.put("realityIrY", realityIrY);   
//		BigDecimal overdueRateY = mbPrdBaseInfo.getOverdueRateY();
////		Double overdue = overdueRateY.doubleValue();
//		map.put("overdue_rate_y", overdueRateY);
//		
//		SignPdfMethod spd = new SignPdfMethod();
//		spd.toPDFWriteValue(map);
//		DecimalFormat format = new DecimalFormat("###0.#########");
//		BigDecimal a  = new BigDecimal(0.095).multiply(new BigDecimal(100));;
//		System.out.println(format.format(a));

		StringBuffer sb = new StringBuffer("");
		sb.append("'11','0102020',");
		sb.append("'12','0102030',");
		sb.append("'13','0102010',");
		sb.append("'14','0102040',");
		sb.append("'15','0102091',");
		sb.append("'16','0102050',");
		sb.append("'17','0102094',");
		sb.append("'18','0102080',");
		sb.append("'19','0102094',");
		sb.append("'1A','0102094',");
		
		sb.append("'21','0101010',");
		sb.append("'22','0101050',");
		sb.append("'23','0101060',");
		sb.append("'24','0102094',");
		sb.append("'25','0101070'");
		System.out.println(sb.toString());
		
	}
	
	//导入个人客户基本信息
	/*
	 * SELECT indiv_sps_name,cus_id,input_id FROM cus_indiv@CMISTEST2101 WHERE length(indiv_sps_name)>5,配偶客户姓名超长需要过滤
	 */
	public static void test1(Connection conn) {
		try {
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_loan_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_loan b  )  ";
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
	public static void test2(Connection conn) throws SQLException {
		PreparedStatement up_prime_rt_knd_ps=null;
		String up_prime_rt_knd=" UPDATE acc_corp_loan a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
		System.out.println(up_prime_rt_knd);
		up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
		up_prime_rt_knd_ps.executeUpdate();
		if(up_prime_rt_knd_ps!=null){
			up_prime_rt_knd_ps.close();
		}
	}
	
	//根据核算数据库更新委托贷款账务信息
	public static void test3(Connection conn) {
		//key:新表字段   
		//value:老表字段
		try {
			//根据核算数据库更新账务信息
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//贷款余额
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//应收正常利息
			map2.put("inner_actl_int", "SETL_NORM_INT");//实收正常利息
			map2.put("overdue_actl_int", "ps_setl_int");//当前欠息金额
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//应收罚息
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//实收罚息
			map2.put("delay_int_cumu", "ps_setl_od_int");//欠罚息金额
			map2.put("inner_int_cumu", "accrued_Int_Nor");//当前计提利息
			map2.put("off_int_cumu", "accrued_Int_Pen");//当前计提罚息
			map2.put("overdue_rece_int", "od_days");//逾期天数
			map2.put("overdue_balance", "od_amt");//逾期本金
			map2.put("over_times_current", "ps_od_num");//当前逾期期数
			map2.put("over_times_total", "ps_od_sum");//累计逾期期数
			map2.put("latest_repay_date", "cur_due_dt");//下一次还款日
//			map2.put("settl_date", "");//结清日期
			map2.put("loan_debt_sts", "loan_debt_sts");//核算账务状态
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
			String ycloans = " SELECT BILL_NO FROM acc_corp_loan_rb a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
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
				String low_risk=" UPDATE acc_corp_loan_rb a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
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
			
			/*
			PreparedStatement up_low_risk=null;
			String low_risk=" UPDATE acc_loan a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no=a.bill_no )  where  a.bill_no in ( select loan_no from v_lm_loan@YCLOANS ) ";
			up_low_risk=conn.prepareStatement(low_risk);
			up_low_risk.execute();
			*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	public static void writeToFile() throws IOException{
		BufferedWriter bufferWriter = null;
		bufferWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("C:/Users/vie/Desktop/de1.csv"),"utf-8"));
		bufferWriter.write("客户号,客户名,机构,用户");
		bufferWriter.write("\n");
		bufferWriter.write("客户号1,客户名1,机构1,用户1");
		bufferWriter.write("\n");
		bufferWriter.flush();
		if(bufferWriter!=null){
			bufferWriter.close();
		}
	}
}
