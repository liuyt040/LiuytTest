package ncmis.data.A_fnc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import ncmis.data.ConnectPro;

/*
 * 查询基础数据表
 * 所有序列重置
 */
public class AQueryBaseTable {
	
	private static Set<String> set = new HashSet<String>() ;

	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url, ConnectPro.username,
				ConnectPro.passwd);
		try {
//			test1(conn);
			test2(conn);
			test3(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (conn != null) {
			conn.close();
		}
	}
	//根据初始库newcmis_init查询数据不为空的表，作为基础配置表
	public static void test1(Connection conn){
		System.out.println("start...");
		String s="SELECT TABLE_NAME FROM user_tables ";
		PreparedStatement query_ps=null;
		ResultSet rs=null;
		try {
			query_ps = conn.prepareStatement(s);
			rs = query_ps.executeQuery();
			int count=0;
			while(rs.next()){
				count++;
				String TABLE_NAME = rs.getString("TABLE_NAME");
				PreparedStatement query_count_ps=null;
				query_count_ps=conn.prepareStatement("select count(*) from "+TABLE_NAME);
				ResultSet count_rs = query_count_ps.executeQuery();
				Object count_r=0;
				while(count_rs.next()){
					count_r=count_rs.getObject(1);
					if(!count_r.toString().equals("0")){
						System.out.println(TABLE_NAME);
					}
				}
				if(count_rs!=null){
					count_rs.close();
				}
				if(query_count_ps!=null){
					query_count_ps.close();
				}
				
			}
			System.out.println(count);
			System.out.println("end...");
			if(rs!=null){
				rs.close();
			}
			if(query_ps!=null){
				query_ps.close();
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	//将基础表插入到set中，test3删除时将不在set中的表数据删除
	public static void test2(Connection conn){
		System.out.println("start...");
		String s="SELECT TABLE_NAME FROM　　 base_table ";
		PreparedStatement query_ps=null;
		ResultSet rs=null;
		try {
			query_ps = conn.prepareStatement(s);
			rs = query_ps.executeQuery();
			int count=0;
			while(rs.next()){
				count++;
				String TABLE_NAME = rs.getString("TABLE_NAME");
				set.add(TABLE_NAME);
				
			}
			System.out.println(count);
			System.out.println("end...");
			if(rs!=null){
				rs.close();
			}
			if(query_ps!=null){
				query_ps.close();
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	//删除基表以外的其他表数据
	public static void test3(Connection conn){
		System.out.println("start...");
		String s="SELECT TABLE_NAME FROM　　 user_tables ";
		PreparedStatement query_ps=null;
		ResultSet rs=null;
		try {
			query_ps = conn.prepareStatement(s);
			rs = query_ps.executeQuery();
			int count=0;
			while(rs.next()){
				count++;
				String TABLE_NAME = rs.getString("TABLE_NAME");
				if(!set.contains(TABLE_NAME)){
					if(!"BASE_TABLE".equals(TABLE_NAME)){
						PreparedStatement de_table_ps=null;
						System.out.println(" truncate table "+TABLE_NAME);
						de_table_ps=conn.prepareStatement(" truncate table "+TABLE_NAME);
						de_table_ps.execute();
						if(de_table_ps!=null){
							de_table_ps.close();
						}
					}
				}
			}
			System.out.println(count);
			System.out.println("end...");
			if(rs!=null){
				rs.close();
			}
			if(query_ps!=null){
				query_ps.close();
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	//将基础表的数据导成insert语句
	public static void test4(Connection conn){
		System.out.println("start...");
		String s="SELECT TABLE_NAME FROM　　 base_table ";
		PreparedStatement query_ps=null;
		ResultSet rs=null;
		try {
			query_ps = conn.prepareStatement(s);
			rs = query_ps.executeQuery();
			int count=0;
			while(rs.next()){
				count++;
				String TABLE_NAME = rs.getString("TABLE_NAME");
				set.add(TABLE_NAME);
				
			}
			System.out.println(count);
			System.out.println("end...");
			if(rs!=null){
				rs.close();
			}
			if(query_ps!=null){
				query_ps.close();
			}
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	/*
	 * 基础表 198
	 */
//	insert into base_table (TABLE_NAME)
//	values ('WF_TASKPOOL_USER');
//
//	insert into base_table (TABLE_NAME)
//	values ('WF_TASKPOOL');
//
//	insert into base_table (TABLE_NAME)
//	values ('WF_STUDIO');
//
//	insert into base_table (TABLE_NAME)
//	values ('WF_CLIENT');
//
//	insert into base_table (TABLE_NAME)
//	values ('WFI_WORKFLOW2ORG');
//
//	insert into base_table (TABLE_NAME)
//	values ('WFI_WORKFLOW2BIZ');
//
//	insert into base_table (TABLE_NAME)
//	values ('WFI_SIGN_CONF');
//
//	insert into base_table (TABLE_NAME)
//	values ('T_GUAR_MODEL');
//
//	insert into base_table (TABLE_NAME)
//	values ('T_GUAR_CLASS_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('TREE_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_USR_ROLE');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_USR_DUTY');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_USR');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_TREE_DIC');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_TIMETASK');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_TABLE_COLUMSHOW_SET');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_SYS_INTERACTION');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_SEQ_CONF');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_ROLE_RULE');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_ROLE');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_RESC_METRO');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_RESC_ACT');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_RESC');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_MULTI_DS');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_MSG');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_METRO_PANEL_ROLE');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_METRO_PANEL');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_METRO_LAYOUT');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_METRO_AREA');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_METRO');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_LAYOUT_SKIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_INSTU');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_IMAGE_CONFIG_DETAIL');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_IMAGE_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_GL_ACCT_NO');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_DUTY');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_DEPT');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_CTRL');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_CONFIG_ITEM');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_CONFIG_GROUP');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_COM_TYP');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_COM_CDE');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_BIZ_ID_PRM');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_BIZ_ID');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_BCH');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_BANK_SERNO');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_AREA_EXT');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_AREA');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_AMOUNT_CAL');
//
//	insert into base_table (TABLE_NAME)
//	values ('S_ACC_BANK');
//
//	insert into base_table (TABLE_NAME)
//	values ('SST_RELATION');
//
//	insert into base_table (TABLE_NAME)
//	values ('SST_INFO_DETAIL');
//
//	insert into base_table (TABLE_NAME)
//	values ('SST_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('SST_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('SHORTCUT_CFG');
//
//	insert into base_table (TABLE_NAME)
//	values ('SHORTCUT_BOOK');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_RULESETVERSION');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_RULESETINFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_RULEREPOS');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_ROLE_REPOS');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_ROLE_ACT');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_OPERATION');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_APPDEPLOY');
//
//	insert into base_table (TABLE_NAME)
//	values ('SF_APPCLASSIFY');
//
//	insert into base_table (TABLE_NAME)
//	values ('RISK_PREV_FLOW_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('REPORT_TEMPLET_INPUT_ELEMENT');
//
//	insert into base_table (TABLE_NAME)
//	values ('REPORT_TEMPLET');
//
//	insert into base_table (TABLE_NAME)
//	values ('REPORT_INIT');
//
//	insert into base_table (TABLE_NAME)
//	values ('REPORT_FILE_URL');
//
//	insert into base_table (TABLE_NAME)
//	values ('RECORD_PMS_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('QRY_TEMPLET');
//
//	insert into base_table (TABLE_NAME)
//	values ('QRY_RESULT');
//
//	insert into base_table (TABLE_NAME)
//	values ('QRY_PARAM_DIC');
//
//	insert into base_table (TABLE_NAME)
//	values ('QRY_PARAM');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_PAGE_SHOW_DTL');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_PAGE_SHOW');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_PAGE_DATA');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_LOAN_TYP_GL_MAP');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_LOAN_MTD');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_BIZ_MODE_CFG');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_BIZ_EXT_POINT');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_BIZ_EXT_ITEM_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_BIZ_EXT_ITEM_PRD');
//
//	insert into base_table (TABLE_NAME)
//	values ('P_BIZ_EXT_ITEM');
//
//	insert into base_table (TABLE_NAME)
//	values ('PUB_HLD');
//
//	insert into base_table (TABLE_NAME)
//	values ('PUB_FILE_TEMP_RECORD');
//
//	insert into base_table (TABLE_NAME)
//	values ('PROC_RULE_SCENE');
//
//	insert into base_table (TABLE_NAME)
//	values ('PROC_RULE_ITEM_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('PROC_RULE_ITEM');
//
//	insert into base_table (TABLE_NAME)
//	values ('PROC_RULE');
//
//	insert into base_table (TABLE_NAME)
//	values ('PREV_RSK_PROJ');
//
//	insert into base_table (TABLE_NAME)
//	values ('PREV_RSK_PLAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_TABLE_MAP');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_SCN_FUN_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_RUL_SET');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_RUL_ELM_TMP');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_RUL_ELEMENT');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_RUL_DTL');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_RATE_STD');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_PV_RISK_SCENE');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_PV_RISK_ITEM_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_PV_RISK_ITEM');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_PREVENT_RISK');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_FINC_PROJ_CONF');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_DOC_LIST_ITEM');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_DOC_LIST');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_DOC_INFO_STD');
//
//	insert into base_table (TABLE_NAME)
//	values ('PRD_BASE_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_TEMP_PARAM');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_TEMP');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_TASK_HIS');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_TASK');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_SEND_MODE_SMS');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_SEND_MODE_MAIL');
//
//	insert into base_table (TABLE_NAME)
//	values ('MSG_SEND_MODE');
//
//	insert into base_table (TABLE_NAME)
//	values ('LMT_TREE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IQP_PRB_LST_DTL');
//
//	insert into base_table (TABLE_NAME)
//	values ('IQP_PRB_LST_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('IQP_PRB_LST');
//
//	insert into base_table (TABLE_NAME)
//	values ('IQP_PRB_LBR_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('IQP_INV_MDL_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_PARA_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT_MEITAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT_JUMIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT_JIAO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT_COPY');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT_111');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPTKEXUE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_OPT');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_MEITAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_KEXUE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_JUMIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_JIAO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_COPY');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL_111');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_MODEL_GROUP_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LVL_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_REL_KEXUE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_MEITAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_JUMIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_JIAO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_COPY');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB_111');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_LIB');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_XIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_MEITAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_KEXUE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_JUMIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_JIAO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_COPY');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO_111');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_MEITAN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_KEXUE');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_JUMIN');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_JIAO');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_COPY');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL_111');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_GROUP_INDEX_REL');
//
//	insert into base_table (TABLE_NAME)
//	values ('IND_CTR_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('INDUSTRY_STD');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_PLN_ITM_PRM');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_PLN_CST_PRM');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_TEMPLATE');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_STYLES');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_ITEMS');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_HEAD_COTES_SM');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_HEAD_COL_SM');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_DEF_FMT');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_CONF_BASE_SM');
//
//	insert into base_table (TABLE_NAME)
//	values ('FNC_BRD_PRJ_PRM');
//
//	insert into base_table (TABLE_NAME)
//	values ('EXCHANGE_RATE');
//
//	insert into base_table (TABLE_NAME)
//	values ('DE_PROC_SET');
//
//	insert into base_table (TABLE_NAME)
//	values ('CUS_INFO_TREE');
//
//	insert into base_table (TABLE_NAME)
//	values ('CUS_HND_DTC');
//
//	insert into base_table (TABLE_NAME)
//	values ('CRS_VAL_CONF_BASE');
//
//	insert into base_table (TABLE_NAME)
//	values ('CACHE_ADDRESS_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_PVP');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_MRK');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_IQP');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_CRR');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_CRL');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_CNT');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_BASE_INF');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_APPRV');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_APP');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_STG_ACP');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_SCN_ROUTER');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_SCN_INF');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_OP_CNF');
//
//	insert into base_table (TABLE_NAME)
//	values ('BMM_BASE_INF');
//
//	insert into base_table (TABLE_NAME)
//	values ('WFI_NODE2BIZ');
//
//	insert into base_table (TABLE_NAME)
//	values ('WFI_OLDCMIS');
//
//	insert into base_table (TABLE_NAME)
//	values ('OLD_CMIS_DATA');
//
//	insert into base_table (TABLE_NAME)
//	values ('DOC_CONFIG');
//
//	insert into base_table (TABLE_NAME)
//	values ('MB_PRD_BASE_INFO');
//
//	insert into base_table (TABLE_NAME)
//	values ('DISC_CONFIG');
}
