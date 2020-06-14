package ncmis.data.org;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ncmis.data.ConnectPro;

public class Sorg2Sbch {

	/*
	 * 注意是否有新增机构
	 */
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
//		conn = DriverManager.getConnection(
//				"jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
//			test1(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	
	public static void test1(Connection conn) {
		PreparedStatement ad_ps = null;
		PreparedStatement de_ps = null;
		PreparedStatement up1_ps = null;
		PreparedStatement up2_ps = null;
		
		String delete = " delete from s_bch a where a.bch_cde!='10000' ";
		String org = " INSERT INTO s_bch a( a.instu_cde,a.bch_cde,a.bch_desc,a.bch_level,a.bch_sup_cde,a.bch_sts,a.core_bch ) "+ 
                     " SELECT '0000',organno,organname,decode(organlevel,'1','01','2','02','3','03','4','04'),suporganno,decode(state,'0','A','1','I'),core_organno FROM S_ORG@CMISTEST2101 b where b.organno!='86048'";
		//设置域内，域外
		String update1 = "UPDATE s_bch b SET b.bch_typ='01' WHERE b.bch_cde IN ( SELECT a.organno FROM S_ORG@CMISTEST2101 a WHERE (a.locate LIKE '%88820%' OR a.locate LIKE '%88810%') ) or b.bch_cde='00000' ";
		String update2 = "UPDATE s_bch b SET b.bch_typ='02' WHERE b.bch_cde IN ( SELECT a.organno FROM S_ORG@CMISTEST2101 a WHERE (a.locate LIKE '%89000%' OR a.locate LIKE '%86000%' OR a.locate LIKE '%85000%' OR a.locate LIKE '%81000%') )";
		try {
			de_ps = conn.prepareStatement(delete);
			de_ps.execute();
			
			ad_ps = conn.prepareStatement(org);
			ad_ps.execute();
			
			up1_ps = conn.prepareStatement(update1);
			up1_ps.execute();
			
			up2_ps = conn.prepareStatement(update2);
			up2_ps.execute();
			if (de_ps != null) {
				de_ps.close();
			}
			if (ad_ps != null) {
				ad_ps.close();
			}
			if (up1_ps != null) {
				up1_ps.close();
			}
			if (up2_ps != null) {
				up2_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
