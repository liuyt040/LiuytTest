package ncmis.data.G_lmt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * 导入同业客户授信协议信息lmt_cont到lmt_st_cont
 * 导入授信台账表 lmt_cont_detail到lmt_st_acc
 * 过滤个人授信
 * 
 */
public class LmtCreatTime {

	private static List<String> list=null;
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			checkNew(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//设置limit_occupy的创建时间
	public static void test1(Connection conn) {
//		String update1 = " UPDATE limit_occupy a SET a.create_time=(SELECT TO_CHAR(sysdate,'YYYY-MM-DD HH24:MI:SS') from  dual) WHERE state='1' ";
		String update1 = " UPDATE limit_occupy a SET a.create_time='2019-12-06 01:01:01'  WHERE state='1' ";
		try {
			PreparedStatement update1_ps = conn.prepareStatement(update1);
			update1_ps.execute();
			if (update1_ps != null) {
				update1_ps.close();
			}
			Thread.sleep(2000);
//			String update2 = " UPDATE limit_occupy a SET a.create_time=(SELECT TO_CHAR(sysdate,'YYYY-MM-DD HH24:MI:SS') from  dual) WHERE state='4' ";
			String update2 = " UPDATE limit_occupy a SET a.create_time='2019-12-06 02:02:02'  WHERE state='4' ";
			PreparedStatement update2_ps = conn.prepareStatement(update2);
			update2_ps.execute();
			if (update2_ps != null) {
				update2_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void checkNew(Connection conn) {
		System.out.println("检查limit_occupy创建时间为空的数据，必须有创建时间");
		String checkB = " select * from  limit_occupy where create_time is NULL ";
		PreparedStatement ps_checkB;
		try {
			ps_checkB = conn.prepareStatement(checkB);
			ResultSet rsB = ps_checkB.executeQuery();
			while (rsB.next()) {
				System.out.println(rsB.getString("id"));
			}
			System.out.println("********");
			if (ps_checkB != null) {
				ps_checkB.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		/*
		 * 校验剩余授信,余额与授信金额是否一致
		 */
		String checkC = " SELECT lmt_serno,item_id,crd_lmt,residual_lmt FROM lmt_cont_details@CMISTEST2101 WHERE item_id NOT IN ( "
				+ " SELECT DISTINCT limit_acc_no FROM ( "
				+ " SELECT ac1.LIMIT_acc_no,ac1.amt1,ac2.amt2,ac1.amt1-ac2.amt2 amt_z,lmt.outstnd_lmt FROM (SELECT LIMIT_acc_no,flag,SUM(sum_amt_z) amt1 FROM item_z_s@CMISTEST2101 WHERE flag='1' GROUP BY LIMIT_acc_no,flag ) ac1 "
				+ " LEFT JOIN  (SELECT LIMIT_acc_no,flag,SUM(sum_amt_z) amt2 FROM item_z_s@CMISTEST2101 WHERE flag='2' GROUP BY LIMIT_acc_no,flag ) ac2 ON ac1.LIMIT_acc_no=ac2.LIMIT_acc_no "
				+ " LEFT JOIN lmt_cont_details@CMISTEST2101 lmt ON ac1.LIMIT_acc_no=lmt.item_id  "
				+ " )  "
				+ " ) AND crd_lmt<>residual_lmt AND lmt_state IN ('00','01','02') AND cus_id LIKE 'c%' ";

		PreparedStatement ps_checkC;
		try {
			ps_checkC = conn.prepareStatement(checkC);
			ResultSet rsa = ps_checkC.executeQuery();
			while (rsa.next()) {
				System.out.println(rsa.getString("lmt_serno") + " "
						+ rsa.getString("item_id") + " " + rsa.getString("crd_lmt")
						+ " " + rsa.getString("residual_lmt"));
			}
			System.out.println("********");
			if (ps_checkC != null) {
				ps_checkC.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

	}

}
