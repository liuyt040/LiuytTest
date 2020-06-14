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
 * 贴现协议 ctr_disc_cont 2 ctr_disc_cont
 */
public class CtrDiscCont2CtrDiscCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_disc_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_disc@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
		map.put("cont_no", "cont_no");//合同编号
		map.put("cont_name", "'贴现协议'");//协议名称
		map.put("sign_dt", "sign_date");//签订日期
		map.put("serno", "serno");//业务流水号
		map.put("iqp_cde", "serno");//申请调查编
		map.put("dc_no", "serno");//批复号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("prd_cde", "decode(bull_type,'1','01021314','2','01021314')");//01021314银票，01021314商票
		map.put("prd_name", "'贴现'");//1一般承兑，2保兑仓
		map.put("cl_typ_sub", "decode(bull_type,'1','01','2','02')");//01银票，02商票
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//是否使用授信
		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("cl_typ", "'03'");//信贷品种
		map.put("lr_ind", "decode(limit_ind,'1','Y','2','N')");//是否低风险业务
		map.put("bill_check_ind", "decode(biz_type,'010002','Y','010001 ','N')");//是否电子票据
		map.put("dis_check_ind", "decode(if_tc,'1','Y','2','N')");//是否先贴后查
		map.put("cont_typ", "'01'");//合同类型
		map.put("counterfoil_num", "bill_qty");//票据数量
		map.put("face_amt_sum", "par_amount");//票面总金额（元）
		map.put("counterfoil_ccy", "'CNY'");//票据币种
		map.put("dis_dt", "sign_date");//贴现日期
		map.put("loan_str_dt", "sign_date");//申请调查日期
		map.put("dis_ir", "discount_ir*12");//贴现年利率
		map.put("pay_int_typ", "decode(pay_mode,'1','2','2','1','3','3','4','4')");//付息方式
		map.put("is_pay_cust_inner_bank", "decode(is_pay_cust_inner_bank,'1','Y','2','N')");//买方付息人是否在我行开户
		map.put("is_third_cust_inner_bank", "decode(is_third_cust_inner_bank,'1','Y','2','N')");//第三方付息人是否在我行开户
		map.put("third_cust_name", "third_cust_name");//第三方付息人名称
		map.put("third_bank_no", "third_bank_no");//第三方付息人开户行行号
		map.put("third_cust_no", "third_cust_no");//第三方付息人客户号
		map.put("third_acct_no", "third_acct_no");//第三方付息人账号
		map.put("third_pay_rate", "third_pay_rate");//第三方付息比例
		map.put("dis_typ", "decode(disc_type,'1','01','2','02')");//贴现方式,01买断，02赎回
		map.put("online_mark", "decode(online_mark,'1','Y','2','N')");//线上清算标致
		map.put("agent_dis_ind", "''");//是否代理贴现
		map.put("agent_dis_ind", "''");//是否代理贴现
		map.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map.put("dirt_name", "direction_name");//投向名称
		map.put("usg_dsc", "use_dec");//用途
		map.put("agr_typ", "agriculture_type");//涉农情况,码值一致
		map.put("agr_use", "decode(agriculture_use,'1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260')");//涉农情况,码值一致
//		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
//		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
//		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		

		map.put("change_dt", "''");//合同变更日期,无用 
		map.put("cont_amt", "''");//协议总金额不启用，以票面总金额为准
			
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
			String de_cus_corp=" delete from ctr_dis where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_dis ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_disc_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_dis a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
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

