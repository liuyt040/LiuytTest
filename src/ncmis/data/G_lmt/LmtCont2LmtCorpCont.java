package ncmis.data.G_lmt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * 导入单一法人用户授信协议信息lmt_cont到lmt_corp_cont
 * 导入授信台账表 lmt_cont_detail到lmt_acc
 * 过滤个人授信
 * 
 */
public class LmtCont2LmtCorpCont {

	private static List<String> list = null;
	private static Map<String, String> map = null;

	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		// conn = DriverManager.getConnection(
		// "jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(ConnectPro.url, ConnectPro.username,
				ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			limitOcc(conn);
			insertOcc(conn);
			deleteOcc(conn);
			reSet(conn);
			checkNew(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (conn != null) {
			conn.close();
		}
	}

	// 导入对公授信协议,担保方式字段：GURT_TYP，需要再次处理，系统还没改造完
	public static void test1(Connection conn) {
		// key:新表字段
		// value:老表字段
		map = new HashMap<String, String>();
		map.put("lmt_cont_no", "lmt_serno");// 授信协议号
		map.put("lmt_corp_no", "lmt_serno");// 申请流水号
		map.put("cus_id", "cus_id");// 客户编号
		map.put("cus_name", "cus_name");// 客户名称
		map.put("cus_typ",
				"decode(cus_type,'211','211','221','221','231','231','241','241','260','251','270','250','299','250')");// 客户类型
		map.put("crd_typ", "'1'");// 授信类型：综合授信
		// **
		map.put("crd_app_typ", "'1'");// 授信申请方式：1新增，2修改，3冻结，4解冻

		map.put("crd_biz_typ", "'01'");// 授信业务类型：01内部授信
		map.put("lmt_ccy", "'CNY'");// 授信业务类型：人民币
		map.put("crd_grade", "decode(cus_level,'11','01','23','02','12','03','24','04','25','05','13','06','26','07','14','08','15','09','16','10','00','11','27','12','37','13','11')");// 评级级别
		map.put("expo_cyc_amt", "crd_totl_sum_amt");// 授信协议金额
		map.put("tnr_typ", "decode(term_type,'01','003','02','002','03','001')");// 期限类型
		map.put("lmt_tnr", "term");// 期限
		map.put("crd_str_dt", "start_date");// 授信起始日期
		map.put("crd_end_dt", "expi_date");// 授信到期日期
		map.put("remarks", "remarks");// 调查人意见
		map.put("project_lmt_flag", "decode(prj_lmt_flg,'1','Y','2','N','')");// 是否项目授信
		map.put("gurt_typ", "assure");//担保方式

		map.put("remarks", "remarks");// 调查人意见
		map.put("crt_usr", "cus_manager");// 登记人
		map.put("main_usr", "cus_manager");// 主管客户经理
		map.put("crt_dt", "update_date");// 登记日期
		map.put("lmt_cont_sts",
				"decode(lmt_state,'00','01','01','01','02','03','03','02','04','02')");// 协议状态

		map.put("instu_cde", "'0000'");// 法人编码

		StringBuffer keys = new StringBuffer("");
		StringBuffer values = new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) {
			keys.append("," + entry.getKey());
			values.append("," + entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp = null;
			String de_cus_corp = " truncate table lmt_corp_cont ";
			de_ps_cus_corp = conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();

			PreparedStatement insert_cus_corp_ps = null;
			String insert_cus_corp = " INSERT INTO lmt_corp_cont ("
					+ keys.toString().substring(1)
					+ ") (SELECT "
					+ values.toString().substring(1)
					+ " FROM lmt_cont@CMISTEST2101 b where b.cus_id like 'c%' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps = conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();

			PreparedStatement up_CRT_USR_ps = null;
			String up_CRT_USR = " UPDATE lmt_corp_cont a SET a.CRT_USR= (SELECT  b.CUST_MGR FROM cus_corp b WHERE b.cus_id=a.cus_id) ";
			up_CRT_USR_ps = conn.prepareStatement(up_CRT_USR);
			up_CRT_USR_ps.execute();
			if (up_CRT_USR_ps != null) {
				up_CRT_USR_ps.close();
			}

			PreparedStatement up_main_br_id = null;
			String main_br_id = " UPDATE lmt_corp_cont a SET crt_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.crt_usr) ";
			up_main_br_id = conn.prepareStatement(main_br_id);
			up_main_br_id.execute();

			PreparedStatement up_input_br_id = null;
			String input_br_id = " UPDATE lmt_corp_cont a SET main_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=crt_usr) ";
			up_input_br_id = conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			PreparedStatement up_gurt_typ_ps = null;
			String up_gurt_typ = " UPDATE lmt_corp_cont a SET a.gurt_typ=replace(a.gurt_typ,'00','40')  ";
			up_gurt_typ_ps = conn.prepareStatement(up_gurt_typ);
			up_gurt_typ_ps.execute();
			if(up_gurt_typ_ps!=null){
				up_gurt_typ_ps.close();
			}

			// 将授信协议预登记状态改为签订
			String up_lmt_cont_sts = " UPDATE lmt_corp_cont a SET a.lmt_cont_sts='01'  WHERE a.lmt_cont_sts='00' ";
			PreparedStatement up_lmt_cont_sts_ps = null;
			System.out.println(up_lmt_cont_sts);
			up_lmt_cont_sts_ps = conn.prepareStatement(up_lmt_cont_sts);
			up_lmt_cont_sts_ps.execute();
			if (up_lmt_cont_sts_ps != null) {
				up_lmt_cont_sts_ps.close();
			}

			if (de_ps_cus_corp != null) {
				de_ps_cus_corp.close();
			}
			if (insert_cus_corp_ps != null) {
				insert_cus_corp_ps.close();
			}
			if (up_main_br_id != null) {
				up_main_br_id.close();
			}
			if (up_input_br_id != null) {
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}

	// 导入对公客户授信台账表，lmt_acc
	public static void test2(Connection conn) {
		// key:新表字段
		// value:老表字段
		// 新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map = new HashMap<String, String>();
		map.put("lmt_acc_no", "item_id");// 额度台帐编号
		map.put("lmt_cont_no", "lmt_serno");// 授信协议编号
		map.put("lmt_dc_no", "lmt_serno");// 分项授信编号，没用
		map.put("dc_no", "lmt_serno");// 授信批复编号，没用
		map.put("lmt_dtl_no", "lmt_serno");// 授信批复编号，没用
		map.put("lmt_corp_no", "lmt_serno");// 申请编号,必须与授信协议中的申请编号保持一致
		map.put("cus_id", "cus_id");// 客户编号
		map.put("cus_name", "cus_name");// 客户名称
		map.put("lmt_ccy", "'CNY'");// 币种
		map.put("crd_amt", "crd_lmt");// 人工调整授信额度（元）
		map.put("out_amt", "outstnd_lmt");// 已用金额（元）
		map.put("avai_amt", "residual_lmt");// 可用金额（元）
		// ***
		// map.put("tnr_typ",
		// "decode(term_type,'01','003','02','002','03','001'");//期限类型
		// map.put("lmt_tnr", "01");//

		map.put("crd_str_dt", "start_date");// 授信起始日
		map.put("crd_end_dt", "expi_date");// 授信到期日

		map.put("crd_app_typ", "'1'");// 授信方式：1新增，2修改
		map.put("acc_sts",
				"decode(lmt_state,'00','01','01','01','02','03','03','02','04','02')");// 台账状态

		StringBuffer keys = new StringBuffer("");
		StringBuffer values = new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) {
			keys.append("," + entry.getKey());
			values.append("," + entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp = null;
			String de_cus_corp = " delete from lmt_acc where 1=1 ";
			de_ps_cus_corp = conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();

			PreparedStatement insert_cus_corp_ps = null;
			String insert_cus_corp = " INSERT INTO lmt_acc ("
					+ keys.toString().substring(1)
					+ ") (SELECT "
					+ values.toString().substring(1)
					+ " FROM lmt_cont_details@CMISTEST2101 b where b.cus_id like 'c%' ) ";
			System.out.println(insert_cus_corp);

			insert_cus_corp_ps = conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();

			PreparedStatement up_main_br_id = null;
			String main_br_id = " UPDATE lmt_acc a SET main_usr= (SELECT  main_usr FROM lmt_corp_cont b WHERE a.LMT_CONT_NO=b.LMT_CONT_NO) ";
			up_main_br_id = conn.prepareStatement(main_br_id);
			up_main_br_id.execute();

			PreparedStatement up_input_br_id = null;
			String input_br_id = " UPDATE lmt_acc a SET main_bch = (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.main_usr) ";
			up_input_br_id = conn.prepareStatement(input_br_id);
			up_input_br_id.execute();

			// 将授信台账预登记状态改为签订
			String up_acc_sts = " UPDATE Lmt_Acc a SET a.acc_sts='01'  WHERE a.acc_sts='00' ";
			PreparedStatement up_acc_sts_ps = null;
			System.out.println(up_acc_sts);
			up_acc_sts_ps = conn.prepareStatement(up_acc_sts);
			up_acc_sts_ps.execute();
			if (up_acc_sts_ps != null) {
				up_acc_sts_ps.close();
			}

			if (de_ps_cus_corp != null) {
				de_ps_cus_corp.close();
			}
			if (insert_cus_corp_ps != null) {
				insert_cus_corp_ps.close();
			}
			if (up_main_br_id != null) {
				up_main_br_id.close();
			}
			if (up_input_br_id != null) {
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
/*
	public static void check(Connection conn) {
		// 校验授信协议和授信台账金额是否一致
		String check_amt = " SELECT a.lmt_cont_no,b.lmt_cont_no,a.expo_cyc_amt,b.crd_amt FROM lmt_corp_cont a "
				+ " LEFT JOIN lmt_acc b ON a.lmt_cont_no=b.lmt_cont_no "
				+ " WHERE a.expo_cyc_amt!=b.crd_amt ";

		PreparedStatement check_amt_ps = null;
		PreparedStatement check_out_ps = null;
		PreparedStatement check_create_time_ps = null;
		try {
			check_amt_ps = conn.prepareStatement(check_amt);
			ResultSet rs = check_amt_ps.executeQuery();
			System.out
					.println("*********开始校验：校验单一法人客户授信协议与授信台账总金额不符的数据*********");
			while (rs.next()) {
				System.out.println(rs.getObject(1) + " " + rs.getObject(2)
						+ " " + rs.getObject(3) + " " + rs.getObject(4));
			}
			System.out
					.println("*********结束校验：校验单一法人客户授信协议与授信台账总金额不符的数据*********");
			if (rs != null) {
				rs.close();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {

			if (check_amt_ps != null) {
				try {
					check_amt_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (check_out_ps != null) {
				try {
					check_out_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

			if (check_create_time_ps != null) {
				try {
					check_create_time_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

	}*/

	public static void limitOcc(Connection conn) throws SQLException {
		
		// 1 删除占用，释放明细
		String item_s = "delete from item_z_s@CMISTEST2101";
		PreparedStatement ps_item_s = conn.prepareStatement(item_s);
		ps_item_s.execute();
		if (ps_item_s != null) {
			ps_item_s.close();
		}
		// 2 根据有效状态的业务合同插入占用明细
		String item_z_cont = " INSERT INTO item_z_s@CMISTEST2101( "
				+ " SELECT cont_no,limit_acc_no, amt_z,'1' FROM ( "
				+ " SELECT cont_no,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL"
				+ " SELECT cont_no,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2'"
				+ " UNION ALL"
				+ " SELECT cont_no,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2'"
				+ " UNION ALL"
				+ " SELECT cont_no,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2'"
				+ " UNION ALL"
				+ " SELECT cont_no,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2')"
				+ "  WHERE amt_z>0)";
		PreparedStatement ps_item_z_cont = conn.prepareStatement(item_z_cont);
		ps_item_z_cont.execute();
		if (ps_item_z_cont != null) {
			ps_item_z_cont.close();
		}

		// 3 根据垫款数据插入占用明细
		/*取消该步骤，晚上日终时会自动从核算系统获取垫款生成的逾期贷款数据，并占用授信
		String item_z_advance = " INSERT INTO item_z_s@CMISTEST2101( "
				+ " SELECT loan_no,limit_acc_no,orig_prcp amt_z,'1' FROM ( "
				+ " SELECT a.loan_no,d.loan_cont_no,a.orig_prcp,a.loan_os_prcp,b.cont_no,c.limit_ind,c.limit_ma_no,c.limit_acc_no FROM lm_loan@YCLOANS a "
				+ " LEFT JOIN lm_loan_cont@YCLOANS d ON a.loan_no=d.loan_no "
				+ " LEFT JOIN acc_accp@CMISTEST2101 b ON a.loan_no=b.accp_no "
				+ " LEFT JOIN ctr_accp_cont@CMISTEST2101 c ON c.cont_no=b.cont_no "
				+ " WHERE a.loan_typ IN ('YC005','BH006') ) )";
		PreparedStatement ps_item_z_advance = conn
				.prepareStatement(item_z_advance);
		ps_item_z_advance.execute();
		if (ps_item_z_advance != null) {
			ps_item_z_advance.close();
		}*/

		// 4 释放贷款，非循环合同，循环合同不释放
		String item_s_cont = " INSERT INTO item_z_s@CMISTEST2101 "
				+ " SELECT cont_no,limit_acc_no,amt_s,'2' FROM ( "
				+ " SELECT a.cont_no,a.bill_no,a.loan_amount-a.loan_balance amt_s,b.limit_acc_no FROM acc_loan@CMISTEST2101 a "
				+ " JOIN ctr_loan_cont@CMISTEST2101 b ON a.cont_no=b.cont_no AND ( b.cont_type!='2' OR b.cont_type IS NULL) "
				+ " WHERE a.account_status IN ('1') AND a.cont_no IN ( "
				+ " SELECT cont_no FROM ( "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2')) "// 正常
				+ " UNION ALL "
				+ " SELECT a.cont_no,a.bill_no,a.loan_amount amt_s,b.limit_acc_no FROM acc_loan@CMISTEST2101 a "
				+ " JOIN ctr_loan_cont@CMISTEST2101 b ON a.cont_no=b.cont_no AND ( b.cont_type!='2' OR b.cont_type IS NULL) "
				+ " WHERE a.account_status IN ('8','9') AND a.cont_no IN ( "
				+ " SELECT cont_no FROM ( "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2')) "
				+ " ) WHERE amt_s > 0 ";
		PreparedStatement ps_item_s_cont = conn.prepareStatement(item_s_cont);
		ps_item_s_cont.execute();
		if (ps_item_s_cont != null) {
			ps_item_s_cont.close();
		}

		// 5 释放银承,状态为结清或垫款（9，2，8,6）,注意追加保证金的数据
		String item_s_accp = " INSERT INTO item_z_s@CMISTEST2101 "
				+ " SELECT cont_no,limit_acc_no,amt_s,'2' FROM ( "
				+ " SELECT a.cont_no,a.bill_no,a.accp_amount-a.security_money_amt amt_s,b.limit_acc_no FROM acc_accp@CMISTEST2101 a "
				+ " JOIN ctr_accp_cont@CMISTEST2101 b ON a.cont_no=b.cont_no  "
				+ " WHERE a.account_status IN ('9','2','8','6') AND a.cont_no IN ( "
				+ " SELECT cont_no FROM ( "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2')) "// 结清，释放票面金额-初始保证金金额
				+ " UNION ALL "
				+ " SELECT a.cont_no,a.bill_no,a.add_security_money_amt amt_s,b.limit_acc_no FROM acc_accp@CMISTEST2101 a "
				+ " JOIN ctr_accp_cont@CMISTEST2101 b ON a.cont_no=b.cont_no  "
				+ " WHERE a.account_status IN ('1') AND a.cont_no IN ( "
				+ " SELECT cont_no FROM ( "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2')) "// 正常，释放追加保证金金额
				+ " ) WHERE amt_s > 0 ";
		PreparedStatement ps_item_s_accp = conn.prepareStatement(item_s_accp);
		ps_item_s_accp.execute();
		if (ps_item_s_accp != null) {
			ps_item_s_accp.close();
		}

		// 6 --释放保函，状态为结清或垫款（9，2，8，6），保函没有追加保证金的数据
		String item_s_cvrg = " INSERT INTO item_z_s@CMISTEST2101 "
				+ " SELECT cont_no,limit_acc_no,amt_s,'2' FROM ( "
				+ " SELECT a.cont_no,a.bill_no,a.guarantee_amount-a.security_money_amt amt_s,b.limit_acc_no FROM acc_cvrg@CMISTEST2101 a "
				+ " JOIN ctr_cvrg_cont@CMISTEST2101 b ON a.cont_no=b.cont_no  "
				+ " WHERE a.account_status IN ('9','2','8','6') AND a.cont_no IN ( "
				+ " SELECT cont_no FROM ( "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM ctr_loan_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_accp_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt-security_money_amt amt_z FROM ctr_cvrg_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,par_amount amt_z FROM ctr_disc_cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,cont_amt amt_z FROM Ctr_ItSmStIn_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " UNION ALL "
				+ " SELECT cont_no,input_id,limit_acc_no,apply_amount*exchange_rate-security_rmb_amount amt_z FROM Ctr_ItSmStout_Cont@CMISTEST2101 WHERE cont_state IN ('200','800') AND limit_ind='2' "
				+ " )) " // --结清，释放保函金额-初始保证金金额，保函不能追加保证金
				+ " ) WHERE amt_s > 0";

		PreparedStatement ps_item_s_cvrg = conn.prepareStatement(item_s_cvrg);
		ps_item_s_cvrg.execute();
		if (ps_item_s_cvrg != null) {
			ps_item_s_cvrg.close();
		}
/*
		// 7 --释放垫款
		String item_s_advance = " INSERT INTO item_z_s@CMISTEST2101( "
				+ " SELECT * FROM (SELECT loan_no,limit_acc_no,orig_prcp-loan_os_prcp amt_s,'2' FROM ( "
				+ " SELECT a.loan_no,d.loan_cont_no,a.orig_prcp,a.loan_os_prcp,b.cont_no,c.limit_ind,c.limit_ma_no,c.limit_acc_no FROM lm_loan@YCLOANS a "
				+ " LEFT JOIN lm_loan_cont@YCLOANS d ON a.loan_no=d.loan_no "
				+ " LEFT JOIN acc_accp@CMISTEST2101 b ON a.loan_no=b.accp_no "
				+ " LEFT JOIN ctr_accp_cont@CMISTEST2101 c ON c.cont_no=b.cont_no "
				+ " WHERE a.loan_typ IN ('YC005','BH006')  "
				+ " )) WHERE amt_s>0)";

		PreparedStatement ps_item_s_advance = conn
				.prepareStatement(item_s_advance);
		ps_item_s_advance.execute();
		if (ps_item_s_advance != null) {
			ps_item_s_advance.close();
		}*/

		// 8856820160031
		// 原系统合同中授信占用错误，将占用授信由2016EDTZ88568000004，改为2016EDTZ88568000002
		String up1 = " update item_z_s@CMISTEST2101 a  set a.limit_acc_no='2016EDTZ88568000002' where a.cont_no='8856820160031' ";

		PreparedStatement ps_up1 = conn.prepareStatement(up1);
		ps_up1.execute();
		if (ps_up1 != null) {
			ps_up1.close();
		}
	}

	public static void insertOcc(Connection conn) throws SQLException,
			InterruptedException {

		String delete = " truncate table limit_occupy ";

		PreparedStatement ps_delete = conn.prepareStatement(delete);
		ps_delete.execute();
		if (ps_delete != null) {
			ps_delete.close();
		}

		String insert = " INSERT INTO limit_occupy( ID,limit_id,limit_item_id,occ_business_seq,occ_amt,occ_stage,state,cus_id,currency,create_time,lmt_blc,last_chg_dt,Instu_Cde,limit_type,crd_typ ) "
				+ " SELECT dbms_random.string('l','45'),b.lmt_serno,a.limit_acc_no,a.cont_no,a.sum_amt_z_s,'1',decode(a.flag,'1','1','2','4'),b.cus_id,'CNY','','','','0000','1','20' FROM (SELECT cont_no,limit_acc_no,flag,SUM(sum_amt_z) sum_amt_z_s FROM item_z_s@CMISTEST2101  GROUP BY cont_no,limit_acc_no,flag ) a "
				+ " LEFT JOIN lmt_cont_details@CMISTEST2101 b ON a.limit_acc_no=b.item_id ";

		PreparedStatement ps_item_s_advance = conn.prepareStatement(insert);
		ps_item_s_advance.execute();
		if (ps_item_s_advance != null) {
			ps_item_s_advance.close();
		}

		String update3 = " UPDATE limit_occupy occ SET occ.lmt_blc = "
				+ " ( "
				+ " SELECT nvl(c.occ_amt,0)-nvl(d.occ_amt,0) blc FROM (SELECT * FROM limit_occupy a WHERE a.state='1' ) c "
				+ " LEFT JOIN (SELECT * FROM limit_occupy b WHERE b.state='4' ) d ON c.limit_item_id=d.limit_item_id AND c.occ_business_seq=d.occ_business_seq "
				+ " WHERE c.limit_item_id=occ.limit_item_id AND c.occ_business_seq=occ.occ_business_seq "
				+ " ) ";
		PreparedStatement update3_ps = conn.prepareStatement(update3);
		update3_ps.execute();
		if (update3_ps != null) {
			update3_ps.close();
		}

	}

	// 删除业务合同号不存在的授信明细，有合同没台账的合同不移植，所以要将其占用明细删除，删除时不能删除垫款的明细
	public static void deleteOcc(Connection conn) throws SQLException,
			InterruptedException {

		String delete = " DELETE FROM Limit_Occupy li WHERE li.occ_business_seq NOT IN ( "
				+ " SELECT a.cont_no FROM ctr_loan_cont a "
				+ " UNION ALL "
				+ " SELECT b.cont_no FROM ctr_fact_cont b "
				+ " UNION ALL "
				+ " SELECT c.cont_no FROM ctr_corp_loan_entr C "
				+ " UNION ALL "
				+ " SELECT d.cont_no FROM ctr_tf_app d "
				+ " UNION ALL "
				+ " SELECT cont_no FROM ctr_accp_cont "
				+ " UNION ALL "
				+ " SELECT cont_no FROM ctr_cvrg_cont "
				+ " UNION ALL "
				+ " SELECT cont_no FROM ctr_loc_cont "
				+ " UNION ALL "
				+ " SELECT cont_no FROM ctr_dis "
				+ " UNION ALL "
				+ " SELECT loan_no FROM lm_loan@ycloans WHERE loan_typ IN ('YC005','BH006'))  ";

		PreparedStatement ps_delete = conn.prepareStatement(delete);
		ps_delete.execute();
		if (ps_delete != null) {
			ps_delete.close();
		}

		/*
		 * 校验移植后新系统根据授信占用明细金额与台账使用金额不一致的数据
		 */
		String checkA = " SELECT lmt.lmt_cont_no,lmt.lmt_acc_no,lmt.out_amt,a.limit_item_id,a.z_amt FROM lmt_acc lmt "
				+ " LEFT JOIN  "
				+ " (SELECT a1.limit_item_id,a1.occ_amt_1,a2.occ_amt_2,a1.occ_amt_1-nvl(a2.occ_amt_2,0) z_amt FROM (SELECT limit_item_id,SUM(occ_amt) occ_amt_1 FROM Limit_Occupy WHERE state='1' GROUP BY limit_item_id ) a1 "
				+ " LEFT JOIN (SELECT limit_item_id,SUM(occ_amt) occ_amt_2 FROM Limit_Occupy WHERE state='4' GROUP BY limit_item_id ) a2 ON a1.limit_item_id=a2.limit_item_id "
				+ " ) a ON a.limit_item_id=lmt.lmt_acc_no AND lmt.out_amt !=a.z_amt "
				+ " WHERE a.limit_item_id IS NOT NULL ORDER BY lmt_cont_no ";

		System.out.println("检验业务占用明细与授信台账的使用金额是否一致，开始......");
		PreparedStatement ps_checkA = conn.prepareStatement(checkA);
		ResultSet rsa = ps_checkA.executeQuery();
		while (rsa.next()) {
			System.out.println(rsa.getString("lmt_cont_no") + " "
					+ rsa.getString("lmt_acc_no") + " "
					+ rsa.getString("out_amt") + " "
					+ rsa.getString("limit_item_id") + " "
					+ rsa.getString("z_amt"));
		}
		System.out.println("********");
		if (ps_checkA != null) {
			ps_checkA.close();
		}
		System.out.println("检验业务占用明细与授信台账的使用金额是否一致，结束......");
	}

	// 根据明细重置台账占用金额
	public static void reSet(Connection conn) throws SQLException,
			InterruptedException {

		String insert_1 = " UPDATE lmt_acc acc SET acc.out_amt= "
				+ " ( "
				+ " SELECT z_amt FROM ( "
				+ " SELECT a1.limit_item_id,a1.occ_amt_1,a2.occ_amt_2,a1.occ_amt_1-nvl(a2.occ_amt_2,0) z_amt FROM (SELECT limit_item_id,SUM(occ_amt) occ_amt_1 FROM Limit_Occupy WHERE state='1' GROUP BY limit_item_id ) a1 "
				+ " LEFT JOIN (SELECT limit_item_id,SUM(occ_amt) occ_amt_2 FROM Limit_Occupy WHERE state='4' GROUP BY limit_item_id ) a2 ON a1.limit_item_id=a2.limit_item_id "
				+ " ) WHERE limit_item_id=acc.lmt_acc_no "
				+ " ) WHERE acc.lmt_acc_no IN ( "
				+ " SELECT a1.limit_item_id FROM (SELECT limit_item_id,SUM(occ_amt) occ_amt_1 FROM Limit_Occupy WHERE state='1' GROUP BY limit_item_id ) a1 "
				+ " LEFT JOIN (SELECT limit_item_id,SUM(occ_amt) occ_amt_2 FROM Limit_Occupy WHERE state='4' GROUP BY limit_item_id ) a2 ON a1.limit_item_id=a2.limit_item_id)  ";

		PreparedStatement ps_insert_1 = conn.prepareStatement(insert_1);
		ps_insert_1.execute();
		if (ps_insert_1 != null) {
			ps_insert_1.close();
		}

		String insert_2 = " UPDATE lmt_acc acc SET acc.out_amt=0 WHERE acc.lmt_acc_no NOT IN ( "
				+ " SELECT a1.limit_item_id FROM (SELECT limit_item_id,SUM(occ_amt) occ_amt_1 FROM Limit_Occupy WHERE state='1' GROUP BY limit_item_id ) a1 "
				+ " LEFT JOIN (SELECT limit_item_id,SUM(occ_amt) occ_amt_2 FROM Limit_Occupy WHERE state='4' GROUP BY limit_item_id ) a2 ON a1.limit_item_id=a2.limit_item_id) ";

		PreparedStatement ps_insert_2 = conn.prepareStatement(insert_2);
		ps_insert_2.execute();
		if (ps_insert_2 != null) {
			ps_insert_2.close();
		}

		String insert_3 = " UPDATE lmt_acc a SET a.avai_amt = a.crd_amt-a.out_amt ";
		PreparedStatement ps_insert_3 = conn.prepareStatement(insert_3);
		ps_insert_3.execute();
		if (ps_insert_3 != null) {
			ps_insert_3.close();
		}
	}

	public static void checkNew(Connection conn) {
		/*
		 * 校验移植后新系统授信占用金额与明细使用金额不一致的数据
		 */
		String checkA = " SELECT lmt.lmt_cont_no,lmt.lmt_acc_no,lmt.out_amt,a.limit_item_id,a.z_amt FROM lmt_acc lmt "
				+ " LEFT JOIN  "
				+ " (SELECT a1.limit_item_id,a1.occ_amt_1,a2.occ_amt_2,a1.occ_amt_1-nvl(a2.occ_amt_2,0) z_amt FROM (SELECT limit_item_id,SUM(occ_amt) occ_amt_1 FROM Limit_Occupy WHERE state='1' GROUP BY limit_item_id ) a1 "
				+ " LEFT JOIN (SELECT limit_item_id,SUM(occ_amt) occ_amt_2 FROM Limit_Occupy WHERE state='4' GROUP BY limit_item_id ) a2 ON a1.limit_item_id=a2.limit_item_id "
				+ " ) a ON a.limit_item_id=lmt.lmt_acc_no AND lmt.out_amt !=a.z_amt "
				+ " WHERE a.limit_item_id IS NOT NULL ORDER BY lmt_cont_no ";
		PreparedStatement ps_checkA;
		System.out.println("根据明细，重置授信台账金额后，检验业务占用明细与授信台账的使用金额是否一致，开始......");
		try {
			ps_checkA = conn.prepareStatement(checkA);
			ResultSet rsa = ps_checkA.executeQuery();
			while (rsa.next()) {
				System.out.println(rsa.getString("lmt_cont_no") + " "
						+ rsa.getString("lmt_acc_no") + " "
						+ rsa.getString("out_amt") + " "
						+ rsa.getString("limit_item_id") + " "
						+ rsa.getString("z_amt"));
			}
			System.out.println("********");
			if (ps_checkA != null) {
				ps_checkA.close();
			}
			System.out.println("根据明细，重置授信台账金额后，检验业务占用明细与授信台账的使用金额是否一致，结束......");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
