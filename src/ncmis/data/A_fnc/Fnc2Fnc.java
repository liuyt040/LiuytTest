package ncmis.data.A_fnc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ncmis.data.ConnectPro;
/*
 * 导入老系统的财报模版及财报数据
 */
public class Fnc2Fnc {

	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		// conn = DriverManager.getConnection(
		// "jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(
				ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			test3(conn);
			test4(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (conn != null) {
			conn.close();
		}
	}
	//导入财报项配置fnc_conf_items
	public static void test1(Connection conn) {
		PreparedStatement de_fnc_ps = null;
		PreparedStatement insert_fnc_ps = null;
		
		String de_fnc =" truncate table fnc_conf_items   ";
		String insert_fnc = " INSERT INTO fnc_conf_items(item_id,item_name,fnc_conf_typ,Instu_Cde) SELECT item_id,item_name,fnc_conf_typ,'0000' FROM fnc_conf_items@"+ConnectPro.DBLink+" ";
		try {
			
			de_fnc_ps=conn.prepareStatement(de_fnc);
			de_fnc_ps.execute();
			
			insert_fnc_ps=conn.prepareStatement(insert_fnc);
			insert_fnc_ps.execute();
			
			if (de_fnc_ps != null) {
				de_fnc_ps.close();
			}
			if (insert_fnc_ps != null) {
				insert_fnc_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	//导入报表样式配置fnc_conf_styles
	public static void test2(Connection conn) {
		PreparedStatement de_fnc_ps = null;
		PreparedStatement insert_fnc_ps = null;
		
		String de_fnc =" truncate table fnc_conf_styles   ";
		String insert_fnc = " INSERT INTO fnc_conf_styles (style_id,fnc_name,fnc_conf_dis_name,fnc_conf_typ,fnc_conf_data_col,fnc_conf_cotes,instu_cde) SELECT style_id,fnc_name,fnc_conf_dis_name,fnc_conf_typ,fnc_conf_data_col,fnc_conf_cotes,'0000' FROM fnc_conf_styles@"+ConnectPro.DBLink+" ";
		try {
			
			de_fnc_ps=conn.prepareStatement(de_fnc);
			de_fnc_ps.execute();
			
			insert_fnc_ps=conn.prepareStatement(insert_fnc);
			insert_fnc_ps.execute();
			
			if (de_fnc_ps != null) {
				de_fnc_ps.close();
			}
			if (insert_fnc_ps != null) {
				insert_fnc_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	//导入报表样式配置fnc_conf_def_fmt
	public static void test3(Connection conn) {
		PreparedStatement de_fnc_ps = null;
		PreparedStatement insert_fnc_ps = null;
		
		String de_fnc =" truncate table fnc_conf_def_fmt   ";
		String insert_fnc = " INSERT INTO fnc_conf_def_fmt (style_id,ITEM_ID,FNC_CONF_ORDER,FNC_CONF_COTES,FNC_CONF_ROW_FLG,FNC_CONF_INDENT,FNC_CONF_PREFIX,FNC_ITEM_EDIT_TYP,FNC_CONF_DISP_AMT,FNC_CONF_CHK_FRM,FNC_CONF_CAL_FRM,FNC_CNF_APP_ROW,FNC_CONF_DISP_TPY) SELECT style_id,ITEM_ID,FNC_CONF_ORDER,FNC_CONF_COTES,FNC_CONF_ROW_FLG,FNC_CONF_INDENT,FNC_CONF_PREFIX,FNC_ITEM_EDIT_TYP,FNC_CONF_DISP_AMT,FNC_CONF_CHK_FRM,FNC_CONF_CAL_FRM,FNC_CNF_APP_ROW,FNC_CONF_DISP_TPY FROM fnc_conf_def_fmt@"+ConnectPro.DBLink+" ";
		try {
			
			de_fnc_ps=conn.prepareStatement(de_fnc);
			de_fnc_ps.execute();
			
			insert_fnc_ps=conn.prepareStatement(insert_fnc);
			insert_fnc_ps.execute();
			
			if (de_fnc_ps != null) {
				de_fnc_ps.close();
			}
			if (insert_fnc_ps != null) {
				insert_fnc_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	//导入财务报表配置fnc_conf_template
	public static void test4(Connection conn) {
		PreparedStatement de_fnc_ps = null;
		PreparedStatement insert_fnc_ps = null;
		
		String de_fnc =" truncate table fnc_conf_template   ";
		String insert_fnc = " INSERT INTO fnc_conf_template(fnc_id,fnc_name,fnc_bs_style_id,fnc_pl_style_id,fnc_cf_style_id,fnc_fi_style_id,instu_cde) SELECT fnc_id,fnc_name,fnc_bs_style_id,fnc_pl_style_id,fnc_cf_style_id,fnc_fi_style_id,'0000' FROM fnc_conf_template@"+ConnectPro.DBLink+" ";
		try {
			
			de_fnc_ps=conn.prepareStatement(de_fnc);
			de_fnc_ps.execute();
			
			insert_fnc_ps=conn.prepareStatement(insert_fnc);
			insert_fnc_ps.execute();
			
			if (de_fnc_ps != null) {
				de_fnc_ps.close();
			}
			if (insert_fnc_ps != null) {
				insert_fnc_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
