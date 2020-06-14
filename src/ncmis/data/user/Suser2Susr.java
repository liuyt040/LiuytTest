package ncmis.data.user;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ncmis.data.ConnectPro;
/*
 * 导入用户
 * 导入岗位
 * 角色不导入
 * 设置用户角色，只设置客户经理(需要业务人员提供明确的人员及角色清单)
 * 设置用户岗位，按老系统设置(新增的信贷工厂岗位需要业务提供明确的人员及岗位清单)
 */
public class Suser2Susr {

	private static List<String> list=null;
	
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

	//导入用户
	public static void test1(Connection conn) {
		PreparedStatement ad_ps = null;
		PreparedStatement de_ps = null;
		
		String delete = " truncate table s_usr ";
		String org = " INSERT INTO S_USR(INSTU_CDE,USR_CDE,EXT_IND,USR_NAME,Usr_Bch,Usr_Sts,Logtyp_Valid)  "+
                     " SELECT '0000',actorno,'N',actorname,orgid,decode(state,'0','I','1','A'),'passwordvalid' FROM s_user@CMISTEST2101 ";
		try {
			de_ps = conn.prepareStatement(delete);
			de_ps.execute();
			
			ad_ps = conn.prepareStatement(org);
			ad_ps.execute();
			
			if (de_ps != null) {
				de_ps.close();
			}
			if (ad_ps != null) {
				ad_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	//导入岗位
	/*
	public static void test2(Connection conn) {
		
		list =new ArrayList<String>();
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G001', '信息复核岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G002', '任务分配岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G003', '征信查询岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G004', '押品估值录入岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G005', '电核岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G006', '家访岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G007', '授信审批管理岗（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G008', '授信审批（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G009', '授信审核管理（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G010', '授信审核（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G011', '合同签约（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G012', '放款申请（信贷工厂）', 'A', '', '', '')");
		list.add("insert into s_duty (INSTU_CDE, DUTY_CDE, DUTY_DESC, DUTY_STS, DUTY_RMK, LAST_CHG_DT, LAST_CHG_USR) values ('0000', 'G013', '放款审核（信贷工厂）', 'A', '', '', '')");
		
		try {
			PreparedStatement de_duty_ps=null;
			String de_duty=" delete from s_duty where 1=1 ";
			de_duty_ps=conn.prepareStatement(de_duty);
			de_duty_ps.execute();
			if(de_duty_ps!=null){
				de_duty_ps.close();
			}
			
			PreparedStatement insert_duty_ps=null;
			String insert_duty=" INSERT INTO s_duty (instu_cde,duty_cde,duty_desc,duty_sts) SELECT '0000',b.dutyno,b.dutyname,'A' FROM s_duty@CMISTEST2101 b ";
			insert_duty_ps=conn.prepareStatement(insert_duty);
			insert_duty_ps.execute();
			if(insert_duty_ps!=null){
				insert_duty_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		
		
		PreparedStatement insert_ps=null;
		for(String insert:list){
			try {
				insert_ps = conn.prepareStatement(insert);
				insert_ps.execute();
				if(insert_ps!=null){
					insert_ps.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}*/
	//导入用户拥有的岗位
	/*
	public static void test3(Connection conn) {
		PreparedStatement de_duty_ps = null;
		PreparedStatement insert_user_duty_ps = null;
		
		String de_user_duty =" delete FROM s_Usr_Duty a   ";
		String insert_user_duty = "INSERT INTO s_Usr_Duty (Instu_Cde,Duty_Cde,Usr_Cde,Sts) SELECT '0000',dutyno,actorno,state FROM s_dutyuser@CMISTEST2101 ";
		try {
			
			de_duty_ps=conn.prepareStatement(de_user_duty);
			de_duty_ps.execute();
			
			insert_user_duty_ps=conn.prepareStatement(insert_user_duty);
			insert_user_duty_ps.execute();
			
			if (de_duty_ps != null) {
				de_duty_ps.close();
			}
			if (insert_user_duty_ps != null) {
				insert_user_duty_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	 * 
	 */
}
