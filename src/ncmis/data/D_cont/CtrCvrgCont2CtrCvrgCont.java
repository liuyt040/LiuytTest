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
 * 保函协议 ctr_cvrg_cont 2 ctr_cvrg_cont
 */
public class CtrCvrgCont2CtrCvrgCont {

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
		String count=" SELECT cus_id,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_cvrg_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_cvrg@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
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
		map.put("cont_str_dt", "start_date");//协议签订日期
		map.put("cont_end_dt", "due_date");//合同到期日
		map.put("iqp_cde", "serno");//申请调查编
		map.put("cl_typ", "'CVRG'");//信贷品种
		map.put("prd_cde", "decode(substr(guarantee_type,1,1),'1','CVR002','2','CVR001')");//CVR001融资保函，CVR002非融资
		map.put("prd_pk", "decode(substr(guarantee_type,1,1),'1','FFFBA8BA014DDACC9FD24FDDF9ED102B','2','FFFBA8BA00AD08DE9FD072921FC660FF')");//CVR001融资保函，CVR002非融资
		map.put("prd_name", "decode(substr(guarantee_type,1,1),'1','非融资性保函','2','融资性保函')");//产品名称
		
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
		map.put("cvrg_type", "decode(guarantee_type,"+sb.toString()+")");//保函类型
		map.put("cont_typ", "'01'");//协议类型
		map.put("way_manage", "decode(limit_ind,'1','1','2','2')");//保函办理方式,全额、差额
		map.put("limit_ind", "decode(limit_ind,'1','1','2','2')");//是否使用授信
		map.put("lmt_cont_no", "limit_ma_no");//授信协议编号
		map.put("lmt_acc_no", "limit_acc_no");//额度台帐编号
		map.put("appl_ccy", "'CNY'");//申请币种
		map.put("appl_amt", "cont_amt");//申请签发金额（元）
		map.put("loan_str_dt", "start_date");//保函起始日
		map.put("due_dt", "due_date");//保函到期日期
		map.put("due_dt", "due_date");//保函到期日期
		map.put("cover_pct", "security_money_rt");//保证金比例
		map.put("cover_amt", "security_money_amt");//保证金金额（元）
		map.put("proc_fee_amt", "proc_fee_amount");//保函手续费(元
		map.put("guar_fee_amt", "lg_fee_amount");//担保手续费(元)
		map.put("exc_rt", "'1'");//汇率
		map.put("appl_amt_cny", "cont_amt");//申请金额折人民币（元）
		map.put("cover_ccy", "'CNY'");//保证金币种
		map.put("cover_rt", "'1'");//保证金汇率
		map.put("cover_amt_cny", "security_money_amt");//保证金折合人民币金额(元)
		map.put("dc_no", "serno");//批复编号
		map.put("corep_no", "corep_no");//合同协议号
		map.put("corep_amt", "trade_cont_amt");//相关合同金额
		map.put("bfcy_name", "bfcy_name");//受益人名称
		map.put("bfcy_no", "''");//受益人账户
		map.put("honour_des", "honour_des");//保函承付条件说明
		map.put("guarantee_pay_way", "decode(guarantee_pay_way,'01','00','02','01')");//付款方式，无条件付款，有条件付款
		map.put("remarks", "cla_suggestion");//付款方式，无条件付款，有条件付款
		map.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map.put("dirt_name", "direction_name");//投向名称
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		
	
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
			String de_cus_corp=" delete from ctr_cvrg_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_cvrg_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_cvrg_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
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

