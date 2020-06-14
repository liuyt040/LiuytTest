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
 * 保函协议 ctr_itsmstout_cont 2 ctr_loc_cont
 */
public class CtrItsmstoutCont2CtrLocCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM Ctr_ItSmStout_Cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_ItSmStout@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
	//导入保函协议
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("serno", "serno");//业务流水号
		map.put("cont_no", "cont_no");//合同编号
		map.put("cont_str_dt", "apply_start_date");//协议签订日期
		map.put("cont_end_dt", "apply_end_date");//合同到期日
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("iqp_cde", "serno");//申请调查编
		map.put("dc_no", "serno");//批复编号
		map.put("cl_typ", "'LC'");//信贷品种
		map.put("prd_cde", "decode(biz_type,'420001','LC02','420003','LC01')");//LC02进口开证，LC01国内证开证
		map.put("prd_name","decode(biz_type,'420001','国际信用证','420003','国内信用证')");//产品名称
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//是否使用授信
		map.put("cl_typ_sub", "'LC'");//信贷品种细分
		map.put("limit_ma_no", "limit_ma_no");//授信协议编号
		map.put("limit_acc_no", "limit_acc_no");//额度台帐编号
		map.put("way_manage", "decode(limit_ind,'1','1','2','2')");//信用证办理方式,全额、差额
		map.put("appl_ccy", "apply_cur_type");//申请币种
		map.put("appl_amt", "open_amt");//开证金额
		map.put("exc_rt", "exchange_rate");//汇率
		map.put("appl_amt_cny", "open_amt*exchange_rate");//申请金额折人民币（元）
		map.put("loc_pay_term", "''");//信用证付款天数
		map.put("sol_rt", "over_ship_rate/100");//溢装比例
		map.put("locdt_type", "decode(lc_type,'001','01','002','02')");//信用证期限类型,01即期，02远期
		map.put("fast_day", "''");//远期天数
		map.put("ioc_dt", "apply_end_date");//信用证有效期
		map.put("max_amt", "open_amt*exchange_rate*(1+over_ship_rate/100)");//信用证最大额（元）
		map.put("cover_ccy", "security_cur_type");//保证金币种
		map.put("cover_rt", "'1'");//保证金汇率
		map.put("cover_pct", "security_money_rt");//保证金比例
		map.put("cover_amt", "security_money_amt");//保证金金额（元）
		map.put("crt_end_dt", "''");//信用证最迟装运日期
		map.put("cover_amt_cny", "security_rmb_amount");//保证金折合人民币金额（元）
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("cont_typ", "'01'");//合同类型，最高发生额
		
		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
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
			String de_cus_corp=" delete from ctr_loc_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_loc_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_itsmstout_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_loc_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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

