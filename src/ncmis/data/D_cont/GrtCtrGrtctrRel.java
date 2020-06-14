package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ncmis.data.ConnectPro;

/*
 * 业务合同，担保合同关系表 grt_loanguar_info 2 grt_ctr_grtctr_rel
 */
public class GrtCtrGrtctrRel {

//	private static List<String> list=null;
//	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
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

	//导入业务合同-担保合同关系表
	public static void test1(Connection conn) {

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from grt_ctr_grtctr_rel where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO GRT_CTR_GRTCTR_REL(pk_id,cont_no,GURT_CONT_NO,GURT_TYP,GURT_CCY,GURT_AMT,REL_STS,WF_APPR_STS ) SELECT dbms_random.string('l','20'),CONT_NO,GUAR_CONT_NO,decode(guar_way,'10000','10','20000','20','30'),'CNY',USED_AMT,'2','000' FROM grt_loanguar_info@CMISTEST2101 ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			

			PreparedStatement vch_cus_id_ps=null;
			String vch_cus_id=" UPDATE GRT_CTR_GRTCTR_REL a SET (a.vch_cus_id,a.vch_cus_name) = ( SELECT b.cus_id,b.cus_name FROM GRT_CTR_CONT b WHERE b.grt_cont_no=a.gurt_cont_no ) WHERE a.vch_cus_id IS NULL";
			System.out.println(vch_cus_id);
			vch_cus_id_ps=conn.prepareStatement(vch_cus_id);
			vch_cus_id_ps.execute();
			if(vch_cus_id_ps!=null){
				vch_cus_id_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

}

