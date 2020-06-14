package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * 合同相关的账号信息
 * 无法取到账号序号
 */
public class CtrPayAcc {

//	private static List<String> list=null;
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			test3(conn);
			test4(conn);
			test5(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入普通贷款、委托贷款、保理、贸易融资合同账号
	public static void test1(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map.put("cont_no", "loan_no");//合同号
		map.put("acct_seq", "acc_sn");//账户序号
		map.put("pay_acct_typ", "decode(acct_typ,'PAYM','01','ACTV','02','TURE_P','04','TURE_I','05')");//账号类型
		map.put("acct_cus_id", "''");//账户人客户号
		map.put("acct_cus_name", "acct_name");//账户人客户名称
		map.put("payback_acct", "acct_no");//账号
		map.put("acct_rpym_ac_ccy", "acct_ccy_cde");//币种
		map.put("pay_bank_no", "''");//账户机构号
		map.put("pay_bank_name", "''");//账户机构名
		map.put("pay_org_no", "''");//账户机构号
		map.put("pay_org_name", "''");//机构名称
		map.put("acc_name", "acct_name");//账户人客户名称
		
		
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
			String de_cus_corp=" delete from ctr_pay_acc   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lm_acct_info@YCLOANS b   where loan_no IN ( "
									+" SELECT a.bill_no FROM acc_loan a " 
									+" UNION ALL "
									+" SELECT b.bill_no FROM acc_corp_loan b " 
									+" UNION ALL "
									+" SELECT c.bill_no FROM acc_fact c "
									+" UNION ALL "
									+" SELECT d.bill_no FROM acc_tf_app d ) )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_ctr_pay_ps=null;
			String up_ctr_pay=" UPDATE ctr_pay_acc a SET a.cont_no = ( SELECT distinct cont_no FROM ( "
				+" SELECT a.bill_no,a.cont_no FROM acc_loan a "
				+" UNION ALL "
				+" SELECT b.bill_no,b.cont_no FROM acc_corp_loan b " 
				+" UNION ALL "
				+" SELECT c.bill_no,c.cont_no FROM acc_fact c "
				+" UNION ALL "
				+" SELECT d.bill_no,d.cont_no FROM acc_tf_app d " 
				+") g WHERE g.bill_no=a.cont_no  ) ";
			System.out.println(up_ctr_pay);
			up_ctr_pay_ps=conn.prepareStatement(up_ctr_pay);
			up_ctr_pay_ps.execute();
			if(up_ctr_pay_ps!=null){
				up_ctr_pay_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
	//导入银承合同账号
	public static void test2(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map.put("cont_no", "cont_no");//合同号
		map.put("payback_acct", "issue_cus_acct");//账号
		map.put("acct_seq", "''");//账户序号,默认活期1
		map.put("pay_acct_typ", "'06'");//账号类型,结算账号
		map.put("acct_cus_id", "cus_id");//账户人客户号
		map.put("acct_cus_name", "cus_name");//账户人客户名称
		map.put("acct_rpym_ac_ccy", "'CNY'");//币种
		map.put("pay_bank_no", "''");//账户机构号（机构号）
		
		map.put("pay_bank_name", "''");//账户机构名
		map.put("pay_org_no", "''");//账户机构号
		map.put("pay_org_name", "''");//机构名称
		map.put("acc_name", "cus_name");//账户人客户名称
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		Map<String,String> map1=new HashMap<String, String>();//保证金账号
		map1.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map1.put("cont_no", "cont_no");//合同号
		map1.put("payback_acct", "security_money_ac_no");//账号
		map1.put("acct_seq", "''");//账户序号,默认活期1
		map1.put("pay_acct_typ", "'03'");//账号类型,保证金账号
		map1.put("acct_cus_id", "cus_id");//账户人客户号
		map1.put("acct_cus_name", "cus_name");//账户人客户名称
		map1.put("acct_rpym_ac_ccy", "'CNY'");//币种
		map1.put("pay_bank_no", "''");//账户机构号（机构号）
		
		map1.put("pay_bank_name", "''");//账户机构名
		map1.put("pay_org_no", "''");//账户机构号
		map1.put("pay_org_name", "''");//机构名称
		map1.put("acc_name", "cus_name");//账户人客户名称
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement in_acc_accp_ps=null;
			String in_acc_accp=" INSERT INTO ctr_pay_acc ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101) ";
			System.out.println(in_acc_accp);
			in_acc_accp_ps=conn.prepareStatement(in_acc_accp);
			in_acc_accp_ps.execute();
			if(in_acc_accp_ps!=null){
				in_acc_accp_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//导入保函合同账号,找新核心要保函的账号信息
	public static void test3(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map.put("cont_no", "cont_no");//合同号
		map.put("payback_acct", "cus_account");//账号
		map.put("acct_seq", "''");//账户序号,默认活期1
		map.put("pay_acct_typ", "'06'");//账号类型,结算账号
		map.put("acct_cus_id", "cus_id");//账户人客户号
		map.put("acct_cus_name", "cus_account_name");//账户人客户名称
		map.put("acct_rpym_ac_ccy", "'CNY'");//币种
		map.put("pay_bank_no", "''");//账户机构号（机构号）
		map.put("pay_bank_name", "''");//账户机构名
		map.put("pay_org_no", "''");//账户机构号
		map.put("pay_org_name", "''");//机构名称
		map.put("acc_name", "cus_account_name");//账户人客户名称
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		Map<String,String> map1=new HashMap<String, String>();//保证金账号
		map1.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map1.put("cont_no", "cont_no");//合同号
		map1.put("payback_acct", "security_money_ac_no");//账号
		map1.put("acct_seq", "''");//账户序号,默认活期1
		map1.put("pay_acct_typ", "'03'");//账号类型,保证金账号
		map1.put("acct_cus_id", "cus_id");//账户人客户号
		map1.put("acct_cus_name", "cus_name");//账户人客户名称
		map1.put("acct_rpym_ac_ccy", "'CNY'");//币种
		map1.put("pay_bank_no", "''");//账户机构号（机构号）
		
		map1.put("pay_bank_name", "''");//账户机构名
		map1.put("pay_org_no", "''");//账户机构号
		map1.put("pay_org_name", "''");//机构名称
		map1.put("acc_name", "cus_name");//账户人客户名称
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM pvp_cvrg_app@CMISTEST2101  WHERE approve_status ='997' AND chargeoff_status='3' ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement in_acc_accp_ps=null;
			String in_acc_accp=" INSERT INTO ctr_pay_acc ("+keys1.toString().substring(1)+") (SELECT "+values1.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101) ";
			System.out.println(in_acc_accp);
			in_acc_accp_ps=conn.prepareStatement(in_acc_accp);
			in_acc_accp_ps.execute();
			if(in_acc_accp_ps!=null){
				in_acc_accp_ps.close();
			}

			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
	//导入贴现合同账号,贴现不涉及保证金
	public static void test4(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map.put("cont_no", "cont_no");//合同号
		map.put("payback_acct", "cus_acct");//账号
		map.put("acct_seq", "''");//账户序号,默认活期1
		map.put("pay_acct_typ", "'06'");//账号类型,结算账号
		map.put("acct_cus_id", "cus_id");//账户人客户号
		map.put("acct_cus_name", "cus_name");//账户人客户名称
		map.put("acct_rpym_ac_ccy", "'CNY'");//币种
		map.put("pay_bank_no", "''");//账户机构号（机构号）
		
		map.put("pay_bank_name", "''");//账户机构名
		map.put("pay_org_no", "''");//账户机构号
		map.put("pay_org_name", "''");//机构名称
		map.put("acc_name", "cus_name");//账户人客户名称
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//导入信用证保证金账号，老系统没有结算账号，结算账号在国结输入
	public static void test5(Connection conn) {

		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("rpym_acct_seq", "dbms_random.string('l','32')");//32位流水号
		map.put("cont_no", "cont_no");//合同号
		map.put("payback_acct", "security_money_ac_no");//账号
		map.put("acct_seq", "''");//账户序号,默认活期1
		map.put("pay_acct_typ", "'03'");//账号类型,结算账号
		map.put("acct_cus_id", "cus_id");//账户人客户号
		map.put("acct_cus_name", "cus_name");//账户人客户名称
		map.put("acct_rpym_ac_ccy", "security_cur_type");//币种
		map.put("pay_bank_no", "''");//账户机构号（机构号）
		
		map.put("pay_bank_name", "''");//账户机构名
		map.put("pay_org_no", "''");//账户机构号
		map.put("pay_org_name", "''");//机构名称
		map.put("acc_name", "cus_name");//账户人客户名称
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_pay_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM pvp_itsmstout_app@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

}

