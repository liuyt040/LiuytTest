package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccLoc2AccLoc {
	
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

	//导入信用证台账
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//台帐编号
		map.put("cont_no", "cont_no");//合同编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("appl_ccy", "apply_cur_type");//申请币种
		map.put("appl_amt", "open_amt");//开证金额
		map.put("appl_amt_cny", "open_amt*exchange_rate");//申请金额折人民币（元）
		map.put("loc_pay_term", "''");//信用证付款天数
		map.put("fast_day", "''");//远期天数
		map.put("ioc_dt", "end_date");//信用证有效期
		map.put("max_amt", "open_amt*exchange_rate*(1+over_ship_rate/100)");//信用证最大额（元）
		map.put("cover_ccy", "security_cur_type");//保证金币种
		map.put("cover_rt", "'1'");//保证金汇率
		map.put("cover_pct", "security_money_rt");//保证金比例
		map.put("cover_amt", "security_money_amt");//保证金金额（元）
		map.put("cover_amt_cny", "security_rmb_amount");//保证金折合人民币金额（元）
		map.put("acc_sts", "decode(account_status,'0','1','1','2','6','3','9','4','7','4')");//台账状态
		map.put("loc_no", "accp_no");//信用证编号
		map.put("cla", "cla");//五级分类
		
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prd_cde", "decode(biz_type,'420001','LC02','420003','LC01')");//LC02进口开证，LC01国内证开证
		map1.put("prd_name","decode(biz_type,'420001','国际信用证','420003','国内信用证')");//产品名称
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map1.put("exc_rt", "exchange_rate");//汇率
		map1.put("sol_rt", "over_ship_rate/100");//溢装比例
		map1.put("limit_ma_no", "limit_ma_no");//授信协议编号
		map1.put("limit_acc_no", "limit_acc_no");//额度台帐编号
	
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "input_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "input_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "input_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "input_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
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
			String de_cus_corp=" truncate table acc_loc  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_loc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_itsmstout@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_acc_loc_ps=null;
			String up_acc_loc=" update acc_loc a set  ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_itsmstout_cont@CMISTEST2101 b  where b.cont_no=a.cont_no )";
			System.out.println(up_acc_loc);
			up_acc_loc_ps=conn.prepareStatement(up_acc_loc);
			up_acc_loc_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_loc a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_loc_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_loc b  )";
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
	
}
