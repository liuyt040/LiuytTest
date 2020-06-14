package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * û�к���ʹ��Ȩ
 * û�к�����
 * û�б���
 * û���ᵥ
 * û�б�������֤
 * û�й�·�շ�Ȩ
 * û��Ӧ�����
 * û�г�����˰�˻�
 * û��ũ������շ�Ȩ
 * û�������շ�Ȩ
 */
public class GrtHDetail2GrtDetail {
	
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			test3(conn);
			test4(conn);
			test5(conn);
			test6(conn);
			test7(conn);
			test8(conn);
			test9(conn);
			test10(conn);
			test11(conn);
			test12(conn);
			test13(conn);
			test14(conn);
			test15(conn);
			test16(conn);
			test17(conn);
			test18(conn);
			test19(conn);
			test20(conn);
			test21(conn);
			test22(conn);
			test23(conn);
			test24(conn);
			test25(conn);
			test26(conn);
			test27(conn);
			test28(conn);
			test29(conn);
			test30(conn);
			test31(conn);
			test32(conn);
			test33(conn);
			test34(conn);
			test35(conn);
			testHingeField(conn);
			insert2CongBiao(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}
	
	
	//����ѺƷ��ϸ��Ϣ-����ʹ��Ȩ
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("LAND_NO", "land_license_no");//����֤��������Ȩ֤�ţ�
		map.put("land_use_qual", "decode(land_nature,'1','01','2','01','3','02','5','02','99')");//����ʹ��Ȩ����
		map.put("LAND_USE_WAY", "decode(land_nature,'1','03','2','01','3','99','4','99','5','99','6','99','99')");//����ʹ��Ȩȡ�÷�ʽ
		map.put("LAND_USE_BEGIN_DATE", "land_book_date");//����ʹ��Ȩʹ��������ʼ����
		map.put("LAND_USE_END_DATE", "land_mature_date");//����ʹ��Ȩʹ�����޵�������
		map.put("LAND_PURP", "''");//������;
		map.put("LAND_USE_AREA", "land_acreage");//����ʹ��Ȩ���
		map.put("LAND_NOTINUSE_TYPE", "''");//������������
		map.put("LAND_EXPLAIN", "''");//����˵��
		map.put("LAND_DETAILADD", "''");//������ϸ��ַ
		map.put("PARCEL_NO", "''");//�ڵغ�
		map.put("PURCHASE_DATE", "''");//��������
		map.put("PURCHASE_ACCNT", "land_remise_amt");//����۸�
		map.put("LAND_UP", "''");//�Ƿ��е��϶�����
		map.put("LAND_UP_TYPE", "''");//����������
		map.put("LAND_BUILD_AMOUNT", "''");//���Ͻ���������
		map.put("LAND_UP_OWNERSHIP_NAME", "''");//����������Ȩ������
		map.put("LAND_UP_OWNERSHIP_SCOPE", "''");//����������Ȩ�˷�Χ
		map.put("LAND_UP_EXPLAIN", "land_sign_circe");//���϶�����˵��
		map.put("LAND_UP_ALL_AREA", "''");//���϶����������
		map.put("USE_CERT_NO", "''");//ʹ��Ȩ��Ѻ�Ǽ�֤��
		map.put("USE_CERT_DEP", "''");//ʹ��Ȩ�Ǽǻ���
		map.put("GUAR_CITE_CHANNEL", "''");//��ǰ����ѺƷ��ֵ��������
		map.put("LAND_P_INFO", "''");//�������ڵض����
		map.put("CUR_TYPE", "''");//����
		map.put("DY_AREA", "''");//��Ѻ���
		map.put("DY_LAND_AREA", "''");//�����ѵ�Ѻ���
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_BUILD_USE  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_BUILD_USE ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Land@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_BUILD_USE a SET a.LAND_DETAILADD = (SELECT area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�ڽ�����
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("LAND_NO", "land_license_no");//����֤��������Ȩ֤�ţ�
		map.put("LAND_USE_QUAL", "decode(land_type,'10013','02','10015','02','10014','99','10016','99','01')");//����ʹ��Ȩ����
		map.put("LAND_USE_WAY", "decode(land_type,'10007','01','10008','01','10009','01','10010','01','10011','01','10012','01','10013','99','10014','99','10015','99','10016','99','03')");//����ʹ��Ȩȡ�÷�ʽ
		map.put("LAND_USE_BEGIN_DATE", "land_book_date");//����ʹ��Ȩʹ��������ʼ����
		map.put("LAND_USE_END_DATE", "land_mature_date");//����ʹ��Ȩʹ�����޵�������
		map.put("LAND_PURP", "''");//������;
		map.put("LAND_EXPLAIN", "''");//����˵��
		map.put("LAND_LICENCE", "build_land_no");//�����õع滮���֤��
		map.put("LAYOUT_LICENCE", "build_project_no");//���蹤�̹滮���֤��
		map.put("CONS_LICENCE", "build_construct_no");//ʩ�����֤��
		map.put("PROJ_START_DATE", "start_date");//������������
		map.put("PLAN_FINISH_DATE", "end_date");//���̿���Ԥ�ƽ�������
		map.put("PROJ_PLAN_ACCNT", "invest_budget");//����Ԥ�������
		map.put("BUSINESS_CONTRACT_NO", "''");//������Լ��ͬ���
		map.put("PURCHASE_DATE", "''");//��ͬǩ������
		map.put("PURCHASE_ACCNT", "''");//��ͬǩ�����
		map.put("BUILD_AREA", "building_area");//�������
		map.put("PREDICT_PROJECT_PEICE", "''");//Ԥ���깤�󹤳��ܼ�ֵ
		map.put("PRO_PRS_DATE", "''");//����ʵ����������
		map.put("PREDICT_FINISH_DATE", "''");//���̿�����������
		map.put("PRO_PROCESS", "''");//������ɳ̶�
		map.put("VALUE_DISCOUNT", "''");//��ֵ�ۿ���
		map.put("SALES_TAX_RATE", "''");//���۷���
		
		
		map.put("MANAGEMENT_FEE_RATE", "''");//�������
		map.put("LAND_VALUE_ADDED_TAX", "''");//������ֵ˰˰��
		map.put("OTHER_RATE", "''");//��������
		map.put("BUSINESS_TAX", "''");//Ӫҵ˰������˰˰��
		map.put("ENGINEERING_BUDGET", "''");//����ʵ��Ԥ��
		map.put("GUAR_CITE_CHANNEL", "''");//��ǰ����ѺƷ��ֵ��������
		
		map.put("LAND_PURP_OTHER", "''");//������;�����ı�����
		map.put("PROJECT_MANAGER", "''");//��Ŀ������
		map.put("DEPARTMENT_NAME", "''");//��Ŀ������׼��λ
		map.put("CONSTRUCT_NAME", "''");//ʩ����λ
		map.put("INPUT_MONEY", "''");//Ŀǰ��Ͷ�ʽ��
		map.put("PROJECT_MONEY_RATE", "''");//��Ŀ�ʱ������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_BUIL_PROJECT  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_BUIL_PROJECT ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Building@CMISTEST2101) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_BUIL_PROJECT a SET (a.PROJECT_NAME,a.DETAILS_ADDRESS) = (SELECT gage_name,area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	//����ѺƷ��ϸ��Ϣ-���ز�-��ס
	public static void test3(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("business_house_no", "''");//���ز�������ͬ���
		map.put("house_land_no", "house_license_no");//����֤��������Ȩ֤����
		map.put("ready_or_period_house", "decode(if_existinghome,'1','1','2','2')");//�ַ�/�ڷ���ʶ
		map.put("build_structure", "decode(building_structure,'01','00','02','00','03','00','04','00','05','02','06','03')");//�����ṹ
		map.put("build_area", "floor_area");//�������(�O
		map.put("activate_years", "substr(finish_date,0,4)");//�������
		map.put("land_no", "land_license_no");//����֤��
		map.put("land_use_qual", "decode(land_use_type,'10013','02','10015','02','10014','99','10016','99','01')");//����ʹ��Ȩ����
		map.put("land_use_end_date", "land_use_date");//����ʹ��Ȩʹ�����޵�������
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_LIVING_ROOM  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_LIVING_ROOM ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Real_Estate@CMISTEST2101 a WHERE a.house_type IN ('10007','10008','10009')  ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_LIVING_ROOM a SET street = (SELECT area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}

	
	//����ѺƷ��ϸ��Ϣ-���ز�-��ס
	public static void test4(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("business_house_no", "''");//���ز�������ͬ���
		map.put("house_land_no", "house_license_no");//����֤��������Ȩ֤����
//		map.put("build_structure", "decode(building_structure,'01','00','02','00','03','00','04','00','05','02','06','03')");//�����ṹ
		map.put("build_area", "floor_area");//�������(�O
		map.put("activate_years", "substr(finish_date,0,4)");//�������
		map.put("land_no", "land_license_no");//����֤��
		map.put("land_use_qual", "decode(land_use_type,'10013','02','10015','02','10014','99','10016','99','01')");//����ʹ��Ȩ����
		map.put("LAND_USE_WAY", "decode(land_use_type,'10007','01','10008','01','10009','01','10010','01','10011','01','10012','01','10013','99','10014','99','10015','99','10016','99','03')");//����ʹ��Ȩȡ�÷�ʽ
		map.put("land_use_end_date", "land_use_date");//����ʹ��Ȩʹ�����޵�������
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_BUSINESS_INDUSTRY_HOUSR  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_BUSINESS_INDUSTRY_HOUSR ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Real_Estate@CMISTEST2101 a WHERE a.house_type not IN ('10007','10008','10009')  ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_BUSINESS_INDUSTRY_HOUSR a SET street = (SELECT area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	
	//����ѺƷ��ϸ��Ϣ-��������-������
	public static void test5(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("vehicle_brand", "traffic_brand");//Ʒ��/��������
		map.put("vehicle_flag_cd", "framework_no");//���ܺ�
		map.put("engine_num", "engine_no");//��������
		map.put("car_card_num", "traffic_no");//���պ���
		map.put("drive_license_no", "drive_no");//��ʻ֤���
		map.put("run_km", "drive_km");//��ʻ��̣�����
		map.put("factory_date", "production_date");//�������ڻ򱨹���
		map.put("invoice_date", "purchase_date");//��Ʊ����
		map.put("buy_price", "purchase_amt");//��Ʊ���(Ԫ)
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_TRAF_CAR  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_TRAF_CAR ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Traffic_Car@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��е�豸
	public static void test6(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("vehicle_brand", "machine_brand");//Ʒ��/��������
		map.put("spec_model", "machine_type||'/'||machine_manufacturer");//�ͺ�/���
		map.put("devices_amount", "num");//�豸����(̨/��)
		map.put("used_times", "used_year");//ʹ�����ޣ��꣩
		map.put("factory_date", "production_date");//�������ڻ򱨹�����
		map.put("invoice_date", "purchase_date");//��Ʊ����
		map.put("buy_price", "purchase_amt");//��Ʊ���(Ԫ)
		map.put("cur_type", "'CNY'");//��Ʊ����
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_MACH_EQUI  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_MACH_EQUI ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Machine@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-����
	public static void test7(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("ship_type", "decode(ship_type,'10001','06','10002','01','10003','99','10004','05','10005','07','10006','99','10007','99','10008','99')");//��������
		map.put("vehicle_brand", "ship_brand||'/'||ship_manufacturer");//Ʒ��/��������
		map.put("loaded_displacement", "ship_tonnage");//������ˮ��(��
		map.put("mature_date", "used_year");//���ʹ�õ�������
		map.put("factory_date", "production_date");//�������ڻ򱨹�����
		map.put("invoice_date", "purchase_date");//��Ʊ����
		map.put("buy_price", "purchase_amt");//��Ʊ���(Ԫ)
		map.put("cur_type", "'CNY'");//��Ʊ����
		map.put("ship_distinguish_no", "ship_name");//����ʶ���-��������
		map.put("ship_register_no", "ship_cert_no");//�����ǼǺ�-��������֤�����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_SHIP  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_SHIP ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Ship@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��Ȩ
	public static void test8(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("land_no", "zhongdi_no");//�ڵغ�
		map.put("wood_type", "decode(holt_type,'10001','�ò���','10002','������','10003','н̿��')");//����
		map.put("little_name", "other_address");//С����
		map.put("com_partment", "lin_ban");//�ְ�
		map.put("sub_compartment", "xiao_ban");//С��
		map.put("main_type", "maior_tree_type");//��Ҫ����
		map.put("wood_num", "tree_num");//����
		map.put("aver_age", "tree_year");//ƽ�����䣨�꣩
		map.put("gain_way", "decode(get_way,'10001','99','10002','99','10003','01')");//ȡ�÷�ʽ
		map.put("purchase_accnt", "get_amt");//�а������Ԫ��
		map.put("use_cert_begin_date", "get_date");//ȡ������
		map.put("use_cert_area", "holt_area*666.6666667");//�ֵ����(�O
		map.put("use_cert_end_date", "mature_end_date");//��ֹ����
		map.put("cur_type", "'CNY'");//�����
		map.put("use_no", "holt_name");//��ȨȨ֤��
		map.put("use_cert_location", "holt_name");//�����ַ
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_USUF_LAND  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_USUF_LAND ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Holt@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_USUF_LAND a SET (a.use_no,a.use_cert_office,a.use_cert_location) = (SELECT b.right_cert_no,b.right_org,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-������ѺƷ
	public static void test9(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("mort_brand", "other_name");//��Ѻ��Ʒ��
		map.put("mort_spec", "other_des||'/'||remark");//�ͺ�
		map.put("mort_quantity", "other_num");//��Ѻ������
		map.put("buy_date", "buy_date");//��������
		map.put("buy_curr_cd", "'CNY'");//�������
		map.put("buy_sum", "buy_amt");//�����Ԫ��
//		map.put("mort_spec", "remark");//�ͺ�-��ע
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_MORTGAGE_OTHER  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_MORTGAGE_OTHER ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Other@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_MORTGAGE_OTHER a SET (a.use_no,a.use_cert_office,a.use_cert_location) = (SELECT b.right_cert_no,b.right_org,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�����Ѻ
	public static void test10(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("cargo_name", "machine_brand");//��������
		map.put("vehicle_brand", "machine_type||'/'||firm");//Ʒ��/����/����
		map.put("cargo_amount", "num");//��������
		map.put("cargo_measure_unit", "decode(unit,'��','01','08')");//��λ����
		map.put("latest_approved_price", "deposit_amt");//���º˶��۸�Ԫ��
		map.put("details_address", "area_location");//��ϸ��ַ
		
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Cargo_Pledge  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Cargo_Pledge ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Stock@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_MORTGAGE_OTHER a SET (a.use_no,a.use_cert_office,a.use_cert_location) = (SELECT b.right_cert_no,b.right_org,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-����ʹ��Ȩ
	public static void test11(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("land_use_area", "land_area");//����ʹ��Ȩ���(�O)
		map.put("land_use_begin_date", "contrac_start_date");//����ʹ��Ȩʹ��������ʼ����
		map.put("land_use_end_date", "contrac_end_date");//����ʹ��Ȩʹ�����޵�������
		map.put("land_explain", "over_condition");//����˵��
		map.put("purchase_accnt", "contrac_amt");//����۸�(Ԫ)
		
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/*
			 * ����ʹ��Ȩ�Ѿ����¹�������ɾ��
			 * PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_BUILD_USE  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			 */
			
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Build_Use ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_G_Land_Right@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Build_Use a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�����
	public static void test12(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("quality_unit", "'01'");//������λ
		map.put("quality", "metal_gram");//����

		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_NOBLE_METAL  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_NOBLE_METAL ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Metal@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_NOBLE_METAL a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-ծȯ
	public static void test13(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("local_bond_no", "code");//ծȯ����
		map.put("issue_name", "publisher");//����������
		map.put("bond_cert_no", "credential_no");//ծȯƾ֤��
		map.put("bond_amt", "denomination");//Ʊ���Ԫ��
		map.put("cur_type", "currency");//����
		map.put("bond_rate", "year_rate");//����(��)
		map.put("issue_date", "start_day");//��������
		map.put("term_date", "end_day");//��������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Bond_Pledge  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Bond_Pledge ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_National_Dbt@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_NOBLE_METAL a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-����-û�б���
	public static void test14(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("publisher_name", "co_name");//����������
		map.put("insur_no", "policy_no");//��������
		map.put("policy_holder_name", "policy_holder");//Ͷ��������
		map.put("acce_insurer_name", "insured");//������������
		map.put("bene_name", "beneficiary");//����������
		map.put("insur_type_cd", "decode(policy_type,'10001','002','10003','004','10004','008','10006','007','10007','006','10008','006','10010','001','10011','011','012')");//��������
		map.put("insur_begin_date", "pay_s_day");//��������
		map.put("insur_end_date", "pay_e_day");//��������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Insu_Slip  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Insu_Slip ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Policy@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Insu_Slip a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-Ʊ��
	public static void test15(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("bill_no", "bill_no");//Ʊ�ݺ���
		map.put("bill_type", "decode(if_electronic_ticket,'1','01','2','02')");//Ʊ������
		map.put("cur_type", "currency");//����
		map.put("bill_amt", "bill_amt");//Ʊ���Ԫ��
		
		//�����˺�
		
		map.put("remitter_date", "start_date");//��Ʊ����
		map.put("bill_end_date", "end_date");//Ʊ�ݵ�������
		map.put("remitter_name", "drawer_name");//��Ʊ������
		map.put("remitter_no", "drawer_ins_code");//��Ʊ����֯��������
		map.put("remitter_bank_no", "drawer_bank_id");//��Ʊ�˿������к�
		map.put("remitter_bank_name", "drawer_bank");//��Ʊ�˿���������
		map.put("remitter_account", "drawer_acc_id");//��Ʊ���˺�
		map.put("payee_name", "receiver_name");//�տ�������
		map.put("accept_type", "decode(acpt_type,'1','06','2','07','3','07','4','07','5','09')");//�ж�������
		map.put("acpt_bank_id", "acpt_bank_id");//�ж����к�
		map.put("accept_name", "acpt_bank_name");//�ж�������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Bill  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Bill ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Bill@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�ᵥ,û���ᵥ
	public static void test16(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("receipts_code", "depot_no");//���ݺ���
		map.put("cargo_ship_date", "depot_date");//����װ������
		map.put("cargo_name", "goods_name");//��������
		map.put("cargo_amount", "goods_num");//������������
		map.put("details_address", "address");//��ϸ��ַ
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Marine_Bl  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Marine_Bl b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Depot@CMISTEST2101 a where  a.depot_type='10002' ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�ֵ�-ȫ��Ϊ��׼�ֵ���û�зǱ�׼�ֵ�
	public static void test17(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("swp_code", "depot_no");//���ݺ���
		map.put("publisher_name", "depot_co");//����������
		map.put("swr_begin_date", "depot_date");//�ֵ���ʼ����
		map.put("cargo_name", "goods_name");//��������
		map.put("swr_price", "goods_all_amt");//��׼�ֵ���ֵ��Ԫ��
		map.put("cur_type", "'CNY'");//����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_SWR  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_SWR b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Depot@CMISTEST2101 a where  a.depot_type='10001' ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��Ʊ-�����й�˾,��ϵͳ��Ʊ����Ϊ10005��10003��10004��Ϊnull�Ķ�Ӧ�����й�˾��
	public static void test18(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("owner_count", "num");//���ʹ�Ȩ��(��)
		map.put("identify_price", "buy_amt");//ÿ���϶���ֵ
		map.put("cur_type", "'CNY'");//����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Stock_Unlisted  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Stock_Unlisted b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Stock@CMISTEST2101 a  where a.stock_type in ('10005','10003','10004') or a.stock_type is null ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��Ʊ-���й�˾,��ϵͳ��Ʊ����Ϊ10001��10002�Ķ�Ӧ���й�˾��
	public static void test19(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("stock_no", "stock_no");//��Ʊ����
		map.put("company_name", "stock_name");//��˾����
		map.put("owner_count", "num");//���ʹ�Ȩ����(��)
		map.put("per_value", "buy_amt");//ÿ���м�
		map.put("guar_sum_amt", "num*buy_amt");//��Ѻ�ܼ�ֵ��Ԫ��
		map.put("cur_type", "'CNY'");//����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_STOCK_LIST  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_STOCK_LIST b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Stock@CMISTEST2101 a  where a.stock_type in ('10001','10002')  ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-����,��ϵͳ��Ʊ����Ϊ10006�Ķ�Ӧ����
	public static void test20(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("fund_no", "stock_no");//�������
		map.put("owner_name", "stock_name");//����������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_STOCK_FUND  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_STOCK_FUND b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Stock@CMISTEST2101 a  where a.stock_type in ('10006')  ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�浥
	public static void test21(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("is_in_receipt", "decode(if_ourself,'1','Y','2','N')");//�Ƿ��д浥
		map.put("receipt_no", "deposit_name");//�浥ƾ֤��
		map.put("account_no", "deposit_name");//�浥ƾ֤��
		map.put("cur_type", "deposit_currency");//����
		map.put("origin_amt", "deposit_amt");//�浥��Ԫ��
		map.put("start_date", "open_date");//��ʼ��
		map.put("end_date", "close_date");//������
		map.put("oth_bank_name", "bank_name");//��������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_DEPO_RECEIPT  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_DEPO_RECEIPT b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Deposit_Rec@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	//����ѺƷ��ϸ��Ϣ-Ӧ���˿�
	public static void test22(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("guaranty_name", "pay_name");//����������
		map.put("face_value_price", "pay_account_no");//Ʊ���Ԫ
		map.put("receipt_no", "invoice_no");//��Ʊ���
		map.put("old_account", "decode(aging_units,'01',aging,'02',aging*30,'03',360*aging,'')");//����(��)
		map.put("pledge_register_no", "peop_bank_reg_no");//������Ѻ�Ǽ�֤�����
		map.put("payment_date", "pay_end_date");//��Ʊ������
		map.put("receipt_date", "fount_date");//��Ʊ����
		map.put("cur_type", "'CNY'");//
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Acc_Rece  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Acc_Rece b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Account_Rec@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-����
	public static void test23(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("pk_id", "cvrg_no");//�������
		map.put("log_type", "decode(cvrg_type,'01','20','02','18')");//��������
		map.put("log_organ_name", "bank_name");//����������������
		map.put("log_amt", "amount");//������Ԫ
		map.put("begin_date", "cvrg_start_date");//��֤��ʼ����
		map.put("end_date", "cvrg_end_date");//��֤������
		map.put("cur_type", "'CNY'");//
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_BACKLETTER  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_BACKLETTER b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Cvrg@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�˻��ʽ�
	public static void test24(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("contract_no", "acc_no");//�˺�
		map.put("start_date", "start_date");//��ʼ����
		map.put("end_date", "end_date");//��ֹ����
		map.put("contract_amt", "balance");//�˻���Ԫ��
		map.put("cur_type", "'CNY'");//����

		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_CONTRACT  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_CONTRACT b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Account@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��Ѻ������
	public static void test25(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("guaranty_name", "other_name");//��Ѻ������
		map.put("impawn_quantity", "other_num");//��Ѻ������
		map.put("buy_sum", "buy_amt");//�����Ԫ��

		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Pledge_Other  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Pledge_Other b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Other@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-������
	public static void test26(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("guaranty_name", "movable_name");//��Ѻ������
		map.put("impawn_quantity", "movable_num");//��Ѻ������
		map.put("buy_sum", "buy_amt");//�����Ԫ��

		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* ǰ���Ѵ����
			 * PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Pledge_Other  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			 */
			
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Pledge_Other b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Movable@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE TInfBill a SET (a.land_no,a.land_detailadd) = (SELECT b.right_cert_no,b.area_location FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-������ת�õ�Ȩ��
	public static void test27(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("reg_org", "book_in_name");//ע��Ǽǻ���
		map.put("rights_begin_date", "start_date");//Ȩ����ʼ����
		map.put("rights_end_date", "end_date");//Ȩ����������
		map.put("remark", "remark");//��ע
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Income  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Income b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Other_Tran_Right@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Income a SET (a.right_reg_no,a.right_name) = (SELECT b.right_cert_no,b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�����ʲ�
	public static void test28(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("details_address", "area_location");//��ϸ��ַ
		map.put("cargo_measure_unit", "decode(asset_units,'01','05','06','01','05','04','08','09','15','07','17','02','99')");//���������λ
		map.put("cargo_amount", "asset_num");//��������
		map.put("agreement_begin_date", "buy_date");//Э����Ч��
		map.put("agreement_end_date", "asset_end_date");//Э�鵽����
		map.put("latest_approved_price", "asset_value");//���º˶��۸�Ԫ��
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/*
			 * ǰ���Ѿ�������������ɾ��
			 * PreparedStatement de_grt_ps=null;
				String de_grt=" TRUNCATE TABLE T_INF_CARGO_PLEDGE  ";
				System.out.println(de_grt);
				de_grt_ps=conn.prepareStatement(de_grt);
				de_grt_ps.execute();
				if(de_grt_ps!=null){
					de_grt_ps.close();
				}
			 */
			
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_CARGO_PLEDGE b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Movable_Asset@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_INF_CARGO_PLEDGE a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��������֤,������
	public static void test29(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("se_credit_no", "loc_no");//��������֤���
		map.put("begin_date", "sign_date");//��֤��ʼ����
		map.put("end_date", "end_date");//��֤��������
		map.put("credit_org_name", "loc_bank");//��֤��������
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Se_Credit  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Se_Credit b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Loc@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-��·�շ�Ȩ,������
	public static void test30(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("charge_ga_code", "charge_no");//�շ�Ȩ���������ĺ�
		map.put("details_address", "highway_name");//��ϸ��
		map.put("rights_begin_date", "start_date");//Ȩ�濪ʼʱ��
		map.put("rights_end_date", "end_date");//Ȩ�浽��ʱ��
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Real_Estate  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Real_Estate b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Highway@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-Ӧ�����,������
	public static void test31(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("fixed_rent_amount", "amount");//ÿ����ȡ�Ĺ̶�����Ԫ��
		map.put("rent_oth_desc", "remark");//������Ϣ����˵��
		
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Rentals_Rece  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Rentals_Rece b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Rent@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-������˰�˻�,������
	public static void test32(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("special_account_no", "rebate_acc");//ר���˻�����
		map.put("exprot_tax_rebate_amount", "amount");//������˰����(Ԫ)
		map.put("pledge_property_desc", "remark");//��Ѻ�Ʋ�����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Acc_Rece_Other  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Acc_Rece_Other b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Taxrebate@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-ũ������շ�Ȩ,������
	public static void test33(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("charge_ga_code", "appro_no");//�շ�Ȩ���������ĺ�
		map.put("details_address", "net_name");//��ϸ��ַ
		map.put("fee_scale_desc", "remark");//�����շѱ�׼����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* ǰ���Ѵ����
			 * PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_REAL_ESTATE  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			 */
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_REAL_ESTATE b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Electric@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-�����շ�Ȩ,������
	public static void test34(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("charge_ga_code", "appro_no");//�շ�Ȩ���������ĺ�
		map.put("fee_scale_desc", "charge_bz");//�����շѱ�׼����
		map.put("fee_scale_desc", "remark");//�����շѱ�׼����
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* ǰ���Ѵ����
			 * PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_INF_REAL_ESTATE  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			 */
			
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_INF_REAL_ESTATE b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_P_Othercharge@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ѺƷ��ϸ��Ϣ-���
	public static void test35(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//ѺƷ���
		map.put("acct_no", "f_account");//�˻�����
		map.put("money_prod_code", "f_prd_code");//��Ʋ�Ʒ����
		map.put("due_date", "f_end_date");//��������
		map.put("pledge_share", "f_amt");//��Ѻ�ݶ�
		map.put("term", "to_date(f_end_date,'yyyy-mm-dd')-to_date(f_start_date,'yyyy-mm-dd')");//��Ʒ���ޣ��죩
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			PreparedStatement de_grt_ps=null;
			String de_grt=" TRUNCATE TABLE T_Inf_Finance_Manage  ";
			System.out.println(de_grt);
			de_grt_ps=conn.prepareStatement(de_grt);
			de_grt_ps.execute();
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO T_Inf_Finance_Manage b ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM Grt_Financial@CMISTEST2101 a   ) ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			PreparedStatement up_grt_ps=null;
			String up_grt=" UPDATE T_Inf_Se_Credit a SET (a.cargo_name) = (SELECT b.gage_name FROM grt_g_basic_info@CMISTEST2101 b WHERE a.guar_no=b.guaranty_id ) ";
			System.out.println(up_grt);
			up_grt_ps=conn.prepareStatement(up_grt);
			up_grt_ps.execute();
			if(up_grt_ps!=null){
				up_grt_ps.close();
			}*/
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	//����ؼ��ֶ�1���ؼ��ֶ�2
	public static void testHingeField(Connection conn) {
		

		String s="SELECT a.guar_type_cd,a.guar_way,a.guar_type1_name,a.guar_type2_name,a.guar_type_cd_cnname,a.keyword1,a.keyword2,a.model_id,b.model_name,b.table_name,b.hinge_field_01,b.hinge_field_02 " 
				 +" FROM t_guar_class_info a "
				 +" LEFT JOIN T_GUAR_MODEL b ON a.model_id=b.model_id AND a.guar_type_state='1' "
				 +" WHERE a.guar_type_state='1' ";
		
		

		try {
			PreparedStatement de_grt_ps=null;
			ResultSet rs = null;
			System.out.println(s);
			de_grt_ps=conn.prepareStatement(s);
			rs=de_grt_ps.executeQuery();
			while(rs.next()){
				String table_name = rs.getString("table_name");
				if(table_name==null||"".equals(table_name)){
					continue;
				}
				String hinge_field_01 = rs.getString("hinge_field_01")==null?"''":rs.getString("hinge_field_01");
				String hinge_field_02 = rs.getString("hinge_field_02")==null?"''":rs.getString("hinge_field_02");
				String guar_type_cd = rs.getString("guar_type_cd");
				
				String up = " update t_guar_base_info a set (a.hinge_field_01,a.hinge_field_02) = ( select "+hinge_field_01+","+hinge_field_02+" from "+table_name+" b where  a.guar_no=b.guar_no ) where a.guar_type_cd='"+guar_type_cd+"' " ;
				System.out.println(up);
				PreparedStatement ps_up = conn.prepareStatement(up);
				ps_up.execute();
				if(ps_up!=null){
					ps_up.close();
				}
			}
			if(rs!=null){
				rs.close();
			}
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
	
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���뽫ѺƷ���������ݣ��ӱ��������ݵĴӱ��в���ѺƷ��ţ��粻���˲����������޷�ά����ϸ��Ϣ
	public static void insert2CongBiao(Connection conn) {
		

		String s="SELECT a.guar_no,a.guar_type_cd,a.guar_type_cd_cnname,b.model_id,c.table_name FROM t_guar_base_info a "
				+" LEFT JOIN t_guar_class_info b ON a.guar_type_cd=b.guar_type_cd "
				+" LEFT JOIN T_GUAR_MODEL c ON b.model_id=c.model_id "
				+" WHERE a.guar_no NOT IN ( "
				+" select guar_no from T_INF_CIVIL_AVIATION union all "
				+" select guar_no from T_INF_MACH_EQUI union all "
				+" select guar_no from T_INF_MORTGAGE_OTHER union all "
				+" select guar_no from T_INF_NOBLE_METAL union all "
				+" select guar_no from T_INF_CONTRACT union all "
				+" select guar_no from T_INF_USUF_LAND union all "
				+" select guar_no from T_INF_BOND_PLEDGE union all "
				+" select guar_no from T_INF_STOCK_UNLISTED union all "
				+" select guar_no from T_INF_TRAF_CAR union all "
				+" select guar_no from T_INF_RENTALS_RECE union all "
				+" select guar_no from T_INF_STOCK_LIST union all "
				+" select guar_no from T_INF_REAL_ESTATE union all "
				+" select guar_no from T_INF_BUIL_VEHICLE union all "
				+" select guar_no from T_INF_DEPO_RECEIPT union all "
				+" select guar_no from T_INF_CAR_CER union all "
				+" select guar_no from T_INF_OTHER_FINANCE union all "
				+" select guar_no from T_INF_INCOME union all "
				+" select guar_no from T_INF_SE_CREDIT union all "
				+" select guar_no from T_INF_SWR union all "
				+" select guar_no from T_INF_OTHER_HOUSE union all "
				+" select guar_no from T_INF_CARGO_PLEDGE union all "
				+" select guar_no from T_INF_STOCK_FUND union all "
				+" select guar_no from T_INF_NON_SWR union all "
				+" select guar_no from T_INF_MARINE_BL union all "
				+" select guar_no from T_INF_BUIL_PROJECT union all "
				+" select guar_no from T_INF_BUSINESS_INDUSTRY_HOUSR union all "
				+" select guar_no from T_INF_ACC_RECE union all "
				+" select guar_no from T_INF_ACC_RECE_OTHER union all "
				+" select guar_no from T_INF_PLEDGE_OTHER union all "
				+" select guar_no from T_INF_FINANCE_MANAGE union all "
				+" select guar_no from T_INF_SHIP union all "
				+" select guar_no from T_INF_BILL union all "
				+" select guar_no from T_INF_INSU_SLIP union all "
				+" select guar_no from T_INF_CAR union all "
				+" select guar_no from T_INF_LIVING_ROOM union all "
				+" select guar_no from T_INF_BACKLETTER union all "
				+" select guar_no from T_INF_BUILD_USE )";
		
		

		try {
			PreparedStatement de_grt_ps=null;
			ResultSet rs = null;
			System.out.println(s);
			de_grt_ps=conn.prepareStatement(s);
			rs=de_grt_ps.executeQuery();
			
			Statement pp = conn.createStatement();
			int count=0;
			while(rs.next()){
				String table_name = rs.getString("table_name");
				String guar_no = rs.getString("guar_no");
				String insert = " insert into  "+table_name+"(GUAR_NO) values('"+guar_no+"')"; 
				count++;
				pp.addBatch(insert);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			if(pp!=null){
				pp.close();
			}
			if(rs!=null){
				rs.close();
			}
			if(de_grt_ps!=null){
				de_grt_ps.close();
			}
			
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
}
