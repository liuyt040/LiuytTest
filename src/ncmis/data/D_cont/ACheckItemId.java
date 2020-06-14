package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import ncmis.data.ConnectPro;

/*
 * 校验银承、保函、信用证、贴现、贷款中是否有生效的合同，但授信台账编号不存在
 */
public class ACheckItemId {

//	private static List<String> list=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
//		conn = DriverManager.getConnection("jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			first(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	public static boolean first(Connection conn){
		String count=" SELECT cont_no,limit_ma_no,limit_acc_no FROM ctr_loan_cont@CMISTEST2101 a WHERE a.cont_state IN ('200','800') AND  A.limit_ind='2' "  
				+" AND a.limit_acc_no NOT IN ( SELECT item_id FROM lmt_cont_details@CMISTEST2101 ) "
				+" UNION ALL "
				+" SELECT cont_no,limit_ma_no,limit_acc_no FROM ctr_accp_cont@CMISTEST2101 a WHERE a.cont_state IN ('200','800') AND  A.limit_ind='2' "  
				+" AND a.limit_acc_no NOT IN ( SELECT item_id FROM lmt_cont_details@CMISTEST2101 ) "
				+" UNION ALL "
				+" SELECT cont_no,limit_ma_no,limit_acc_no FROM ctr_cvrg_cont@CMISTEST2101 a WHERE a.cont_state IN ('200','800') AND  A.limit_ind='2' "  
				+" AND a.limit_acc_no NOT IN ( SELECT item_id FROM lmt_cont_details@CMISTEST2101 ) "
				+" UNION ALL "
				+" SELECT cont_no,limit_ma_no,limit_acc_no FROM ctr_disc_cont@CMISTEST2101 a WHERE a.cont_state IN ('200','800') AND  A.limit_ind='2' "  
				+" AND a.limit_acc_no NOT IN ( SELECT item_id FROM lmt_cont_details@CMISTEST2101 ) "
				+" UNION ALL "
				+" SELECT cont_no,limit_ma_no,limit_acc_no FROM ctr_Itsmstout_cont@CMISTEST2101 a WHERE a.cont_state IN ('200','800') AND  A.limit_ind='2' "  
				+" AND a.limit_acc_no NOT IN ( SELECT item_id FROM lmt_cont_details@CMISTEST2101 ) ";
		boolean cont_num=true;
		PreparedStatement count_ps=null;
		try {
			count_ps=conn.prepareStatement(count);
			ResultSet rs =count_ps.executeQuery();
			while(rs.next()){
				cont_num=false;
				System.out.println("合同号："+rs.getString("CONT_NO")+" 授信台账："+rs.getString("LIMIT_ACC_NO")+" 授信协议："+rs.getString("LIMIT_MA_NO"));
			}
			if(!cont_num){
				System.out.println("老系统存在合同已签订，但授信台账不存在的数据");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}	
		return cont_num;
	}
}

