package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccAccp2AccAccp {
	
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

	//导入银行承兑汇票台账，acc_accp 2 acc_accp
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("cont_no", "cont_no");//合同编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("draft_no", "accp_no");//汇票号码
		map.put("appl_ccy", "'CNY'");//申请币种
		map.put("appl_amt", "accp_amount");//票面金额
		map.put("pre_sign_dt", "accp_issue_date");//票据签发日期
		map.put("pre_due_dt", "accp_due_date");//票据到期日期
		map.put("cushion_flag", "decode(account_status,'6','Y','N')");//是否垫款
		map.put("cushion_amt", "''");//垫款金额
		map.put("security_money_rt", "(security_money_amt+add_security_money_amt)/accp_amount");//保证金比例
		map.put("security_money_amt", "security_money_amt+add_security_money_amt");//保证金金额（元
		map.put("not_hegotiable_flg", "''");//不得转让标记
		map.put("issue_name", "cus_name");//出票人名称
		map.put("issue_bank_cde", "''");//出票人组织机构代码
		map.put("issue_bank_id", "issue_bank_id");//出票人开户行号
		map.put("issue_bank_name", "issue_bank_name");//出票人开户行名
		map.put("issue_bank_acct", "issue_cus_acct");//出票人开户行账号
		map.put("rec_name", "receiver_name");//收款人名称
		map.put("rec_bank_id", "receiver_bank_id");//收款人开户行行号
		map.put("rec_bank_name", "receiver_bank");//收款人开户行行名
		map.put("rec_acct", "receiver_acc_id");//收款人开户行账号
		map.put("acpt_bank_typ", "decode(acpt_bank_type,'1','2','2','3','3','4','4','5','5','6')");//承兑行类型
		map.put("acpt_bank_id", "acpt_bank_id");//承兑行行号
		map.put("acpt_bank_name", "acpt_bank_name");//承兑行名称
		map.put("acpt_bank_cde", "''");//承兑人组织机构代码
		map.put("acpt_bank_acct", "''");//承兑人开户行账号
		map.put("draft_place", "''");//票据签发地
		map.put("lastest_upd_id", "cus_manager");//最新修改人
		map.put("lastest_upd_date", "latest_date");//最新修改日期
		map.put("acc_no", "dbms_random.string('l','30')");//台帐编号
		map.put("pvp_loan_no", "''");//出账申请编号
		map.put("pvp_loan_no", "''");//出账申请编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("acc_sts", "decode(account_status,'0','1','1','2','6','3','9','4','2','5','7','4')");//台账状态
		map.put("cla", "cla");//五级分类
		//合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("iqp_cde", "serno");//申请调查编
		map1.put("serno", "serno");//业务流水号
		map1.put("tnm_time_typ", "decode(biz_type,'212200','02','010000','01')");//票据类型，01电票，02纸票
		map1.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map1.put("dirt_name", "direction_name");//投向名称
		map1.put("remarks", "acpt_use");//承兑用途
		map1.put("prd_cde", "decode(special_type,'1','ACC0101','2','ACC0102')");//1一般承兑，2保兑仓
		map1.put("prd_name", "decode(special_type,'1','银行承兑汇票','2','保兑仓融资')");//1一般承兑，2保兑仓
		
		
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "accp_issue_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "accp_issue_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "accp_issue_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
		map.put("main_bch", "input_br_id");//主管机构
		map.put("iqp_dt", "accp_issue_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
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
			String de_cus_corp=" truncate table acc_accp ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_accp ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_accp@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_accp a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_accp a set ("+keys1.toString().substring(1)+") =(SELECT "+values1.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101 b where b.cont_no= a.cont_no)";
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			//将prd_cde为空的数据默认为ACC0101 Y88048201600014
			PreparedStatement up_prd_cde_ps=null;
			String prd_cde=" update acc_accp a set prd_cde = 'ACC0101' where prd_cde is null";
			up_prd_cde_ps=conn.prepareStatement(prd_cde);
			up_prd_cde_ps.execute();
			if(up_prd_cde_ps!=null){
				up_prd_cde_ps.close();
			}
			
			//替换回车，换行符
			PreparedStatement up_rec_bank_name_ps=null;
			String up_rec_bank_name=" UPDATE acc_accp a SET a.rec_bank_name = replace(rec_bank_name,CHR(10),'') ";
			up_rec_bank_name_ps=conn.prepareStatement(up_rec_bank_name);
			up_rec_bank_name_ps.execute();
			if(up_rec_bank_name_ps!=null){
				up_rec_bank_name_ps.close();
			}
			
			//替换回车，换行符
			PreparedStatement up_rec_bank_name_1_ps=null;
			String up_rec_bank_name_1=" UPDATE acc_accp a SET a.rec_bank_name = replace(rec_bank_name,CHR(13),'') ";
			up_rec_bank_name_1_ps=conn.prepareStatement(up_rec_bank_name_1);
			up_rec_bank_name_1_ps.execute();
			if(up_rec_bank_name_1_ps!=null){
				up_rec_bank_name_1_ps.close();
			}
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_accp_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_accp b  ) ";
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
