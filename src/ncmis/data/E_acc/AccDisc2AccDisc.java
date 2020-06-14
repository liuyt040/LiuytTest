package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * 贴现协议 acc_disc 2 acc_disc
 */
public class AccDisc2AccDisc {

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

	//导入贴现台账
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//台帐编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("acc_sts", "decode(account_status,'00','1','10','2','20','4','21','4')");//台帐状态
		map.put("cont_no", "cont_no");//合同编号
		map.put("change_dt", "''");//合同变更日期,无用 
		map.put("cl_typ", "'03'");//信贷品种
		map.put("cla", "cla");//五级分类
		map.put("appl_amt", "bill_amount");//票面金额（元）
		map.put("appl_ccy", "'CNY'");//申请币种
		map.put("draft_no", "draft_no");//汇票号码
		map.put("bill_str_dt", "bill_issuing_date");//票据签发日
		map.put("bill_end_dt", "bill_due_day");//票据到期日
		map.put("not_hegotiable_flg", "not_hegotiable_flg");//不得转让标记
		map.put("bill_issue", "bill_issue");//票据签发地
		map.put("interest", "disc_interest");//总利息
		map.put("paymoney", "cash_amount");//实付金额
//		map.put("settl_date", "latest_date");//结清日期
		//从合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("serno", "serno");//业务流水号
		map1.put("dc_no", "serno");//批复号
		map1.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//协议状态
		map1.put("sign_dt", "sign_date");//签订日期
		map1.put("prd_cde", "decode(bull_type,'1','01021314','2','01021314')");//01021314银票，01021314商票
		map1.put("prd_name", "'贴现'");//1一般承兑，2保兑仓
		map1.put("cl_typ_sub", "decode(bull_type,'1','01','2','02')");//01银票，02商票
		map1.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//是否使用授信
		map1.put("cont_name", "'贴现协议'");//协议名称
		map1.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map1.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map1.put("lr_ind", "decode(limit_ind,'1','Y','2','N')");//是否低风险业务
		map1.put("bill_check_ind", "decode(biz_type,'010002','Y','010001 ','N')");//是否电子票据
		map1.put("dis_check_ind", "decode(if_tc,'1','Y','2','N')");//是否先贴后查
		map1.put("cus_name", "cus_name");//客户名称
		map1.put("cus_id", "cus_id");//客户编号
		map1.put("cont_typ", "'01'");//合同类型
		map1.put("dis_typ", "decode(disc_type,'1','01','2','02')");//贴现方式,01买断，02赎回
		map1.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map1.put("dirt_name", "direction_name");//投向名称
		map1.put("usg_dsc", "use_dec");//用途
		map1.put("agr_typ", "agriculture_type");//涉农情况,码值一致
		map1.put("agr_use", "decode(agriculture_use,'1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260')");//涉农情况,码值一致
//		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
//		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
//		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("agent_dis_ind", "''");//是否代理贴现
			
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "disc_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "disc_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "disc_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "disc_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
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
			String de_cus_corp=" truncate table acc_disc ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_disc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_disc@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_disc a set ("+keys1.toString().substring(1)+") =(SELECT "+values1.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101 b where b.cont_no= a.cont_no)";
			System.out.println(up_cont);
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_disc a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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
			String delte_ctr_loan_cont=" DELETE FROM ctr_dis a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_disc b  ) ";
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

