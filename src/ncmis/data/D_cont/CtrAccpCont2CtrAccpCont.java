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
 * 银承协议 ctr_accp_cont 2 ctr_accp_cont
 */
public class CtrAccpCont2CtrAccpCont {

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
			if(first(conn)){
				test1(conn);
			}else{
				System.out.println("老系统存在合同已签订，但未放款的合同，需要全部做注销处理后才能移植，否则会造成授信出错");
			}
			
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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_accp_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_accp@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
	
	//导入银承协议
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("sign_dt", "sign_date");//协议签订日期
		map.put("cont_no", "cont_no");//合同编号
		map.put("serno", "serno");//业务流水号
		map.put("cont_end_dt", "pre_due_date");//合同到期日
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("cl_typ", "'ACCP'");//信贷品种
		map.put("prd_cde", "decode(special_type,'1','ACC0101','2','ACC0102')");//1一般承兑，2保兑仓
		map.put("prd_name", "decode(special_type,'1','银行承兑汇票','2','保兑仓融资')");//1一般承兑，2保兑仓
		map.put("tnm_time_typ", "decode(biz_type,'212200','02','010000','01')");//票据类型，01电票，02纸票
		map.put("cont_typ", "'03'");//合同类型
		map.put("acpt_typ", "decode(acpt_type,'1','01','2','02')");//签发类型,自签、代签
		map.put("acpt_typ", "decode(acpt_type,'1','01','2','02')");//签发类型,自签、代签
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//是否使用授信
		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("way_manage", "decode(acpt_treat_mode,'1','1','2','2')");//承兑办理方式,1全额 2敞口
		map.put("appl_ccy", "'CNY'");//申请币种
		map.put("appl_amt", "cont_amt");//申请签发金额（元）
		map.put("cover_pct", "security_money_rt");//保证金比例
		map.put("cover_amt", "security_money_amt");//保证金金额（元）
		map.put("proc_fee_percent", "fee/10000");//手续费率
		map.put("proc_fee_amt", "cont_amt*fee/10000");//手续费金额
		map.put("risk_open_rt", "proc_fee_percent");//敞口承诺费率
		map.put("risk_open_amt", "proc_fee_amount");//敞口承诺费金额（元）
		map.put("overdue_rt", "overdue_rate/10000");//垫款日罚息率
		map.put("dc_no", "serno");//批复编号
		map.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map.put("dirt_name", "direction_name");//投向名称
		map.put("dirt_name", "direction_name");//投向名称
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("acpt_bank_typ", "decode(acpt_bank_type,'1','2','2','3','3','4','4','5','5','6')");//承兑行类型
		map.put("acpt_bank_id", "acpt_bank_id");//承兑行行号
		map.put("acpt_bank_name", "acpt_bank_name");//承兑行行名
		map.put("acpt_use", "acpt_use");//承兑用途
		map.put("remarks", "loan_type_ext2");//备注
		map.put("remarks", "loan_type_ext2");//备注
		map.put("iqp_cde", "serno");//申请调查编
		
		map.put("change_dt", "''");//合同变更日期,无用
		map.put("cont_name", "''");//合同变更日期,无用

		
		
		
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
			String de_cus_corp=" delete from ctr_accp_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_accp_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_accp_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_accp_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//更新10条已结清的银承授信方式为单笔单批
			PreparedStatement up_limit_ind_ps=null;
			String up_limit_ind=" UPDATE ctr_accp_cont a SET a.limit_ind='1' WHERE a.cont_no IN ('D86018201500021','D86018201500022','D86018201600003','Y86018201500172','Y86508201400004','Y88038201400124','Y88038201400186','Y88038201500011','Y88088201500081','Y88088201500098') ";
			up_limit_ind_ps=conn.prepareStatement(up_limit_ind);
			up_limit_ind_ps.execute();
			if(up_limit_ind_ps!=null){
				up_limit_ind_ps.close();
			}
			
			//更新银承协议的协办人
			PreparedStatement up_coopr_usr_ps=null;
			String up_coopr_usr=" UPDATE ctr_accp_cont a SET a.coopr_usr = ( SELECT b.investigator_id FROM iqp_accp_app@CMISTEST2101 B WHERE b.serno= a.serno) ";
			up_coopr_usr_ps=conn.prepareStatement(up_coopr_usr);
			up_coopr_usr_ps.execute();
			if(up_coopr_usr_ps!=null){
				up_coopr_usr_ps.close();
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

