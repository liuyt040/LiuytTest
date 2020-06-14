package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * 保函协议 acc_cvrg 2 acc_cvrg
 */
public class AccCvrg2AccCvrg {

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

	//导入保函协议
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("acc_no", "dbms_random.string('l','30')");//台帐编号
		map.put("bill_no", "bill_no");//借据编号
		map.put("cvrg_no", "bill_no");//保函编号
		map.put("cont_no", "cont_no");//合同编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("cus_id", "cus_id");//客户编号
		map.put("acc_sts", "decode(account_status,'0','1','1','2','3','4','6','3','9','4')");//台账状态
		map.put("cont_start_dt", "start_date");//保函起始日期
		map.put("pre_due_dt", "due_date");//保函终止日期
		map.put("stpint_dt", "start_date");//保函开出日期
		map.put("appl_ccy", "'CNY'");//申请币种
		map.put("appl_amt", "guarantee_amount");//申请金额
		map.put("cover_pct", "security_money_rt");//保证金比例
		map.put("cover_amt", "security_money_amt");//保证金金额（元）
		map.put("day_rt", "day_rate/100");//保证金金额（元）
		map.put("exc_rt", "'1'");//汇率
		map.put("cla", "cla");//五级分类
		//合同
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("dc_no", "serno");//批复编号
		map1.put("prd_cde", "decode(substr(guarantee_type,1,1),'1','CVR002','2','CVR001')");//CVR001融资保函，CVR002非融资
		map1.put("prd_name", "decode(substr(guarantee_type,1,1),'1','融资性保函','2','非融资性保函')");//产品名称
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
		map1.put("cvrg_type", "decode(guarantee_type,"+sb.toString()+")");//保函类型
		map1.put("cvrg_deal_type", "decode(limit_ind,'1','1','2','2')");//保函类型
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//主担保方式
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//担保方式1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//担保方式2
		map1.put("proc_fee_amt", "proc_fee_amount");//保函手续费(元
		map1.put("guar_fee_amt", "lg_fee_amount");//担保手续费(元)
		map1.put("dirt_cde", "loan_direction");//贷款投向代码dirt_cde
		map1.put("dirt_name", "direction_name");//投向名称

		map.put("opr_usr", "cus_manager");//经办人
		map.put("opr_bch", "input_br_id");//经办机构
		map.put("opr_dt", "start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("coopr_usr", "cus_manager");//协办人
		map.put("coopr_bch", "input_br_id");//协办机构
		map.put("coopr_dt", "start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("crt_usr", "cus_manager");//登记人
		map.put("crt_bch", "input_br_id");//登记机构
		map.put("crt_dt", "start_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		map.put("main_usr", "cus_manager");//主管人
		map.put("main_bch", "input_br_id");//主管机构
//		map.put("iqp_dt", "sign_date");//经办时间，先按合同签订时间刷一遍，将为空的再按申请时间刷一遍
		
		
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
			String de_cus_corp=" truncate table acc_cvrg   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_cvrg ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_cvrg@CMISTEST2101 b  )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//更新对公证件类型、证件号码
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_cvrg a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			PreparedStatement up_cont_ps=null;
			String up_cont=" update acc_cvrg a set  ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_cvrg_cont@CMISTEST2101 b where b.cont_no=a.cont_no )";
			System.out.println(up_cont);
			up_cont_ps=conn.prepareStatement(up_cont);
			up_cont_ps.execute();
			if(up_cont_ps!=null){
				up_cont_ps.close();
			}
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			//删除没有台账的合同（未出作废的，在途的）
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_cvrg_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_cvrg b  )";
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

