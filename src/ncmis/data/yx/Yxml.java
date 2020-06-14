package ncmis.data.yx;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ncmis.data.ConnectPro;

/*
 * 抽取老信贷系统中的影像目录编号
 */
public class Yxml {

	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url, ConnectPro.username,
				ConnectPro.passwd);
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
	/*
	public static void test(Connection conn){
		String sql=" SELECT a.flag,a.doc_level,a.doc_name FROM doc_config@CMISTEST101 a  ";
		PreparedStatement ps=null;
		ResultSet rs = null;
		Set set = new HashSet();
		List<String> list=new ArrayList<String>();
		try {
			System.out.println("**start**");
			ps = conn.prepareStatement(sql);
			rs=ps.executeQuery();
			while(rs.next()){
				String s1=(String)rs.getObject(1);
				String s2=(String)rs.getObject(2);
				String s3=(String)rs.getObject(3);
				String [] ss=s2.split(",");
				for(String s:ss){
					System.out.println(" INSERT INTO doc_config_r(flag,doc_level,doc_name) VALUES('"+s1+"'"+","+"'"+s+"','"+s3+"'"+") ;");
				}
				
			}
			System.out.println("**end**");
			if(rs!=null){
				rs.close();
			}
			if(ps!=null){
				ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		
		
	}*/
	
	public static void test1(Connection conn){
		
		
		try {
			PreparedStatement ps=null;
			String sql=" truncate table image_seq  ";
			ps = conn.prepareStatement(sql);
			ps.executeQuery();
			if(ps!=null){
				ps.close();
			}
			
			PreparedStatement ps1=null;
			String sql1=" INSERT INTO image_seq SELECT doc_no,doc_type,doc_name,biz_no,cus_id,cus_name,doc_status,archive_date,markup_date,'00000','',av_Id FROM Archives_Manage@CMISTEST2101 b WHERE b.doc_type='XD01' AND length(b.cus_id)=10 ";
			ps1 = conn.prepareStatement(sql1);
			ps1.executeQuery();
			if(ps1!=null){
				ps1.close();
			}
			
			PreparedStatement ps3=null;
			String sql3=" UPDATE image_seq a SET a.file_level='1,2,4' WHERE a.doc_type='XD01' AND a.cus_id LIKE 'c%' ";
			ps3 = conn.prepareStatement(sql3);
			ps3.executeQuery();
			if(ps3!=null){
				ps3.close();
			}
			
			PreparedStatement ps4=null;
			String sql4=" UPDATE image_seq a SET a.file_level='1,3,4' WHERE a.doc_type='XD01' AND a.cus_id LIKE 'p%'  ";
			ps4 = conn.prepareStatement(sql4);
			ps4.executeQuery();
			if(ps4!=null){
				ps4.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		
		
	}

}
;