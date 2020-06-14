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
 * 没有海域使用权
 * 没有航空器
 * 没有保单
 * 没有提单
 * 没有备用信用证
 * 没有公路收费权
 * 没有应收租金
 * 没有出口退税账户
 * 没有农村电网收费权
 * 没有其他收费权
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
	
	
	//导入押品详细信息-土地使用权
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("LAND_NO", "land_license_no");//土地证（不动产权证号）
		map.put("land_use_qual", "decode(land_nature,'1','01','2','01','3','02','5','02','99')");//土地使用权性质
		map.put("LAND_USE_WAY", "decode(land_nature,'1','03','2','01','3','99','4','99','5','99','6','99','99')");//土地使用权取得方式
		map.put("LAND_USE_BEGIN_DATE", "land_book_date");//土地使用权使用年限起始日期
		map.put("LAND_USE_END_DATE", "land_mature_date");//土地使用权使用年限到期日期
		map.put("LAND_PURP", "''");//土地用途
		map.put("LAND_USE_AREA", "land_acreage");//土地使用权面积
		map.put("LAND_NOTINUSE_TYPE", "''");//闲置土地类型
		map.put("LAND_EXPLAIN", "''");//土地说明
		map.put("LAND_DETAILADD", "''");//土地详细地址
		map.put("PARCEL_NO", "''");//宗地号
		map.put("PURCHASE_DATE", "''");//购买日期
		map.put("PURCHASE_ACCNT", "land_remise_amt");//购买价格
		map.put("LAND_UP", "''");//是否有地上定着物
		map.put("LAND_UP_TYPE", "''");//定着物种类
		map.put("LAND_BUILD_AMOUNT", "''");//地上建筑物项数
		map.put("LAND_UP_OWNERSHIP_NAME", "''");//定着物所有权人名称
		map.put("LAND_UP_OWNERSHIP_SCOPE", "''");//定着物所有权人范围
		map.put("LAND_UP_EXPLAIN", "land_sign_circe");//地上定着物说明
		map.put("LAND_UP_ALL_AREA", "''");//地上定着物总面积
		map.put("USE_CERT_NO", "''");//使用权抵押登记证号
		map.put("USE_CERT_DEP", "''");//使用权登记机关
		map.put("GUAR_CITE_CHANNEL", "''");//当前评估押品价值引用渠道
		map.put("LAND_P_INFO", "''");//土地所在地段情况
		map.put("CUR_TYPE", "''");//币种
		map.put("DY_AREA", "''");//抵押面积
		map.put("DY_LAND_AREA", "''");//土地已抵押面积
		
		
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
	
	//导入押品详细信息-在建工程
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("LAND_NO", "land_license_no");//土地证（不动产权证号）
		map.put("LAND_USE_QUAL", "decode(land_type,'10013','02','10015','02','10014','99','10016','99','01')");//土地使用权性质
		map.put("LAND_USE_WAY", "decode(land_type,'10007','01','10008','01','10009','01','10010','01','10011','01','10012','01','10013','99','10014','99','10015','99','10016','99','03')");//土地使用权取得方式
		map.put("LAND_USE_BEGIN_DATE", "land_book_date");//土地使用权使用年限起始日期
		map.put("LAND_USE_END_DATE", "land_mature_date");//土地使用权使用年限到期日期
		map.put("LAND_PURP", "''");//土地用途
		map.put("LAND_EXPLAIN", "''");//土地说明
		map.put("LAND_LICENCE", "build_land_no");//建设用地规划许可证号
		map.put("LAYOUT_LICENCE", "build_project_no");//建设工程规划许可证号
		map.put("CONS_LICENCE", "build_construct_no");//施工许可证号
		map.put("PROJ_START_DATE", "start_date");//工程启动日期
		map.put("PLAN_FINISH_DATE", "end_date");//工程竣工预计交付日期
		map.put("PROJ_PLAN_ACCNT", "invest_budget");//工程预计总造价
		map.put("BUSINESS_CONTRACT_NO", "''");//工程契约合同编号
		map.put("PURCHASE_DATE", "''");//合同签订日期
		map.put("PURCHASE_ACCNT", "''");//合同签订金额
		map.put("BUILD_AREA", "building_area");//建筑面积
		map.put("PREDICT_PROJECT_PEICE", "''");//预计完工后工程总价值
		map.put("PRO_PRS_DATE", "''");//工程实际启动日期
		map.put("PREDICT_FINISH_DATE", "''");//工程竣工交付日期
		map.put("PRO_PROCESS", "''");//工程完成程度
		map.put("VALUE_DISCOUNT", "''");//价值折扣率
		map.put("SALES_TAX_RATE", "''");//销售费率
		
		
		map.put("MANAGEMENT_FEE_RATE", "''");//管理费率
		map.put("LAND_VALUE_ADDED_TAX", "''");//土地增值税税率
		map.put("OTHER_RATE", "''");//其他费率
		map.put("BUSINESS_TAX", "''");//营业税及附加税税率
		map.put("ENGINEERING_BUDGET", "''");//工程实际预算
		map.put("GUAR_CITE_CHANNEL", "''");//当前评估押品价值引用渠道
		
		map.put("LAND_PURP_OTHER", "''");//土地用途其他文本输入
		map.put("PROJECT_MANAGER", "''");//项目开发人
		map.put("DEPARTMENT_NAME", "''");//项目立项批准单位
		map.put("CONSTRUCT_NAME", "''");//施工单位
		map.put("INPUT_MONEY", "''");//目前已投资金额
		map.put("PROJECT_MONEY_RATE", "''");//项目资本金比例
		
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
	
	
	//导入押品详细信息-房地产-居住
	public static void test3(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("business_house_no", "''");//房地产买卖合同编号
		map.put("house_land_no", "house_license_no");//房产证（不动产权证）号
		map.put("ready_or_period_house", "decode(if_existinghome,'1','1','2','2')");//现房/期房标识
		map.put("build_structure", "decode(building_structure,'01','00','02','00','03','00','04','00','05','02','06','03')");//建筑结构
		map.put("build_area", "floor_area");//建筑面积(㎡
		map.put("activate_years", "substr(finish_date,0,4)");//建成年份
		map.put("land_no", "land_license_no");//土地证号
		map.put("land_use_qual", "decode(land_use_type,'10013','02','10015','02','10014','99','10016','99','01')");//土地使用权性质
		map.put("land_use_end_date", "land_use_date");//土地使用权使用年限到期日期
		
		
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

	
	//导入押品详细信息-房地产-商住
	public static void test4(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("business_house_no", "''");//房地产买卖合同编号
		map.put("house_land_no", "house_license_no");//房产证（不动产权证）号
//		map.put("build_structure", "decode(building_structure,'01','00','02','00','03','00','04','00','05','02','06','03')");//建筑结构
		map.put("build_area", "floor_area");//建筑面积(㎡
		map.put("activate_years", "substr(finish_date,0,4)");//建成年份
		map.put("land_no", "land_license_no");//土地证号
		map.put("land_use_qual", "decode(land_use_type,'10013','02','10015','02','10014','99','10016','99','01')");//土地使用权性质
		map.put("LAND_USE_WAY", "decode(land_use_type,'10007','01','10008','01','10009','01','10010','01','10011','01','10012','01','10013','99','10014','99','10015','99','10016','99','03')");//土地使用权取得方式
		map.put("land_use_end_date", "land_use_date");//土地使用权使用年限到期日期
		
		
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
	
	
	
	//导入押品详细信息-机动车辆-机动车
	public static void test5(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("vehicle_brand", "traffic_brand");//品牌/生产厂商
		map.put("vehicle_flag_cd", "framework_no");//车架号
		map.put("engine_num", "engine_no");//发动机号
		map.put("car_card_num", "traffic_no");//牌照号码
		map.put("drive_license_no", "drive_no");//行驶证编号
		map.put("run_km", "drive_km");//行驶里程（公里
		map.put("factory_date", "production_date");//出厂日期或报关日
		map.put("invoice_date", "purchase_date");//发票日期
		map.put("buy_price", "purchase_amt");//发票金额(元)
		
		
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
	
	//导入押品详细信息-机械设备
	public static void test6(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("vehicle_brand", "machine_brand");//品牌/生产厂商
		map.put("spec_model", "machine_type||'/'||machine_manufacturer");//型号/规格
		map.put("devices_amount", "num");//设备数量(台/套)
		map.put("used_times", "used_year");//使用年限（年）
		map.put("factory_date", "production_date");//出厂日期或报关日期
		map.put("invoice_date", "purchase_date");//发票日期
		map.put("buy_price", "purchase_amt");//发票金额(元)
		map.put("cur_type", "'CNY'");//发票币种
		
		
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
	
	//导入押品详细信息-船舶
	public static void test7(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("ship_type", "decode(ship_type,'10001','06','10002','01','10003','99','10004','05','10005','07','10006','99','10007','99','10008','99')");//船舶类型
		map.put("vehicle_brand", "ship_brand||'/'||ship_manufacturer");//品牌/生产厂商
		map.put("loaded_displacement", "ship_tonnage");//满载排水量(吨
		map.put("mature_date", "used_year");//设计使用到期日期
		map.put("factory_date", "production_date");//出厂日期或报关日期
		map.put("invoice_date", "purchase_date");//发票日期
		map.put("buy_price", "purchase_amt");//发票金额(元)
		map.put("cur_type", "'CNY'");//发票币种
		map.put("ship_distinguish_no", "ship_name");//船舶识别号-船舶名称
		map.put("ship_register_no", "ship_cert_no");//船舶登记号-船舶国籍证书号码
		
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
	
	//导入押品详细信息-林权
	public static void test8(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("land_no", "zhongdi_no");//宗地号
		map.put("wood_type", "decode(holt_type,'10001','用材林','10002','经济林','10003','薪炭林')");//林种
		map.put("little_name", "other_address");//小地名
		map.put("com_partment", "lin_ban");//林班
		map.put("sub_compartment", "xiao_ban");//小班
		map.put("main_type", "maior_tree_type");//主要树种
		map.put("wood_num", "tree_num");//株数
		map.put("aver_age", "tree_year");//平均树龄（年）
		map.put("gain_way", "decode(get_way,'10001','99','10002','99','10003','01')");//取得方式
		map.put("purchase_accnt", "get_amt");//承包或购入金额（元）
		map.put("use_cert_begin_date", "get_date");//取得日期
		map.put("use_cert_area", "holt_area*666.6666667");//林地面积(㎡
		map.put("use_cert_end_date", "mature_end_date");//终止日期
		map.put("cur_type", "'CNY'");//人民币
		map.put("use_no", "holt_name");//林权权证号
		map.put("use_cert_location", "holt_name");//座落地址
		
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
	
	//导入押品详细信息-其他抵押品
	public static void test9(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("mort_brand", "other_name");//抵押物品牌
		map.put("mort_spec", "other_des||'/'||remark");//型号
		map.put("mort_quantity", "other_num");//抵押物数量
		map.put("buy_date", "buy_date");//购入日期
		map.put("buy_curr_cd", "'CNY'");//购入币种
		map.put("buy_sum", "buy_amt");//购入金额（元）
//		map.put("mort_spec", "remark");//型号-备注
		
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
	
	//导入押品详细信息-存货抵押
	public static void test10(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("cargo_name", "machine_brand");//货物名称
		map.put("vehicle_brand", "machine_type||'/'||firm");//品牌/厂家/产地
		map.put("cargo_amount", "num");//货物数量
		map.put("cargo_measure_unit", "decode(unit,'吨','01','08')");//单位类型
		map.put("latest_approved_price", "deposit_amt");//最新核定价格（元）
		map.put("details_address", "area_location");//详细地址
		
		
		
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
	
	//导入押品详细信息-土地使用权
	public static void test11(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("land_use_area", "land_area");//土地使用权面积(㎡)
		map.put("land_use_begin_date", "contrac_start_date");//土地使用权使用年限起始日期
		map.put("land_use_end_date", "contrac_end_date");//土地使用权使用年限到期日期
		map.put("land_explain", "over_condition");//土地说明
		map.put("purchase_accnt", "contrac_amt");//购买价格(元)
		
		
		
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
			 * 土地使用权已经更新过，不能删除
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
	
	//导入押品详细信息-贵金属
	public static void test12(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("quality_unit", "'01'");//质量单位
		map.put("quality", "metal_gram");//质量

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
	
	//导入押品详细信息-债券
	public static void test13(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("local_bond_no", "code");//债券代码
		map.put("issue_name", "publisher");//发行人名称
		map.put("bond_cert_no", "credential_no");//债券凭证号
		map.put("bond_amt", "denomination");//票面金额（元）
		map.put("cur_type", "currency");//币种
		map.put("bond_rate", "year_rate");//利率(年)
		map.put("issue_date", "start_day");//发行日期
		map.put("term_date", "end_day");//到期日期
		
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
	
	//导入押品详细信息-保单-没有保单
	public static void test14(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("publisher_name", "co_name");//发行人名称
		map.put("insur_no", "policy_no");//保单号码
		map.put("policy_holder_name", "policy_holder");//投保人名称
		map.put("acce_insurer_name", "insured");//被保险人名称
		map.put("bene_name", "beneficiary");//受益人名称
		map.put("insur_type_cd", "decode(policy_type,'10001','002','10003','004','10004','008','10006','007','10007','006','10008','006','10010','001','10011','011','012')");//保险险种
		map.put("insur_begin_date", "pay_s_day");//发行日期
		map.put("insur_end_date", "pay_e_day");//到期日期
		
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
	
	//导入押品详细信息-票据
	public static void test15(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("bill_no", "bill_no");//票据号码
		map.put("bill_type", "decode(if_electronic_ticket,'1','01','2','02')");//票据类型
		map.put("cur_type", "currency");//币种
		map.put("bill_amt", "bill_amt");//票面金额（元）
		
		//网银账号
		
		map.put("remitter_date", "start_date");//出票日期
		map.put("bill_end_date", "end_date");//票据到期日期
		map.put("remitter_name", "drawer_name");//出票人名称
		map.put("remitter_no", "drawer_ins_code");//出票人组织机构代码
		map.put("remitter_bank_no", "drawer_bank_id");//出票人开户行行号
		map.put("remitter_bank_name", "drawer_bank");//出票人开户行名称
		map.put("remitter_account", "drawer_acc_id");//出票人账号
		map.put("payee_name", "receiver_name");//收款人名称
		map.put("accept_type", "decode(acpt_type,'1','06','2','07','3','07','4','07','5','09')");//承兑人类型
		map.put("acpt_bank_id", "acpt_bank_id");//承兑人行号
		map.put("accept_name", "acpt_bank_name");//承兑人名称
		
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
	
	//导入押品详细信息-提单,没有提单
	public static void test16(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("receipts_code", "depot_no");//单据号码
		map.put("cargo_ship_date", "depot_date");//货物装运日期
		map.put("cargo_name", "goods_name");//货物名称
		map.put("cargo_amount", "goods_num");//货物账面数量
		map.put("details_address", "address");//详细地址
		
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
	
	//导入押品详细信息-仓单-全部为标准仓单，没有非标准仓单
	public static void test17(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("swp_code", "depot_no");//单据号码
		map.put("publisher_name", "depot_co");//发行人名称
		map.put("swr_begin_date", "depot_date");//仓单起始日期
		map.put("cargo_name", "goods_name");//货物名称
		map.put("swr_price", "goods_all_amt");//标准仓单价值（元）
		map.put("cur_type", "'CNY'");//币种
		
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
	
	//导入押品详细信息-股票-非上市公司,老系统股票类型为10005、10003、10004或为null的对应非上市公司表
	public static void test18(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("owner_count", "num");//出质股权数(股)
		map.put("identify_price", "buy_amt");//每股认定价值
		map.put("cur_type", "'CNY'");//币种
		
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
	
	//导入押品详细信息-股票-上市公司,老系统股票类型为10001、10002的对应上市公司表
	public static void test19(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("stock_no", "stock_no");//股票代码
		map.put("company_name", "stock_name");//公司名称
		map.put("owner_count", "num");//出质股权数额(股)
		map.put("per_value", "buy_amt");//每股市价
		map.put("guar_sum_amt", "num*buy_amt");//质押总价值（元）
		map.put("cur_type", "'CNY'");//币种
		
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
	
	//导入押品详细信息-基金,老系统股票类型为10006的对应基金
	public static void test20(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("fund_no", "stock_no");//基金代码
		map.put("owner_name", "stock_name");//发行人名称
		
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
	
	//导入押品详细信息-存单
	public static void test21(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("is_in_receipt", "decode(if_ourself,'1','Y','2','N')");//是否本行存单
		map.put("receipt_no", "deposit_name");//存单凭证号
		map.put("account_no", "deposit_name");//存单凭证号
		map.put("cur_type", "deposit_currency");//币种
		map.put("origin_amt", "deposit_amt");//存单余额（元）
		map.put("start_date", "open_date");//起始日
		map.put("end_date", "close_date");//到期日
		map.put("oth_bank_name", "bank_name");//银行名称
		
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
	
	
	//导入押品详细信息-应收账款
	public static void test22(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("guaranty_name", "pay_name");//购货商名称
		map.put("face_value_price", "pay_account_no");//票面金额（元
		map.put("receipt_no", "invoice_no");//发票编号
		map.put("old_account", "decode(aging_units,'01',aging,'02',aging*30,'03',360*aging,'')");//账龄(天)
		map.put("pledge_register_no", "peop_bank_reg_no");//人行质押登记证明编号
		map.put("payment_date", "pay_end_date");//发票到期日
		map.put("receipt_date", "fount_date");//发票日期
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
	
	//导入押品详细信息-保函
	public static void test23(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("pk_id", "cvrg_no");//保函编号
		map.put("log_type", "decode(cvrg_type,'01','20','02','18')");//保函类型
		map.put("log_organ_name", "bank_name");//保函开立机构名称
		map.put("log_amt", "amount");//保函金额（元
		map.put("begin_date", "cvrg_start_date");//保证开始日期
		map.put("end_date", "cvrg_end_date");//保证结束日
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
	
	//导入押品详细信息-账户资金
	public static void test24(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("contract_no", "acc_no");//账号
		map.put("start_date", "start_date");//起始日期
		map.put("end_date", "end_date");//截止日期
		map.put("contract_amt", "balance");//账户余额（元）
		map.put("cur_type", "'CNY'");//币种

		
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
	
	//导入押品详细信息-质押其他类
	public static void test25(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("guaranty_name", "other_name");//质押物名称
		map.put("impawn_quantity", "other_num");//质押物数量
		map.put("buy_sum", "buy_amt");//购入金额（元）

		
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
	
	//导入押品详细信息-动产类
	public static void test26(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("guaranty_name", "movable_name");//质押物名称
		map.put("impawn_quantity", "movable_num");//质押物数量
		map.put("buy_sum", "buy_amt");//购入金额（元）

		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* 前面已处理过
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
	
	//导入押品详细信息-其他可转让的权利
	public static void test27(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("reg_org", "book_in_name");//注册登记机构
		map.put("rights_begin_date", "start_date");//权利起始日期
		map.put("rights_end_date", "end_date");//权利到期日期
		map.put("remark", "remark");//备注
		
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
	
	//导入押品详细信息-流动资产
	public static void test28(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("details_address", "area_location");//详细地址
		map.put("cargo_measure_unit", "decode(asset_units,'01','05','06','01','05','04','08','09','15','07','17','02','99')");//货物计量单位
		map.put("cargo_amount", "asset_num");//货物数量
		map.put("agreement_begin_date", "buy_date");//协议生效日
		map.put("agreement_end_date", "asset_end_date");//协议到期日
		map.put("latest_approved_price", "asset_value");//最新核定价格（元）
		
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
			 * 前面已经新增过，不能删除
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
	
	//导入押品详细信息-备用信用证,无数据
	public static void test29(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("se_credit_no", "loc_no");//备用信用证编号
		map.put("begin_date", "sign_date");//保证开始日期
		map.put("end_date", "end_date");//保证结束日期
		map.put("credit_org_name", "loc_bank");//开证机构名称
		
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
	
	//导入押品详细信息-公路收费权,无数据
	public static void test30(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("charge_ga_code", "charge_no");//收费权政府批文文号
		map.put("details_address", "highway_name");//详细地
		map.put("rights_begin_date", "start_date");//权益开始时间
		map.put("rights_end_date", "end_date");//权益到期时间
		
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
	
	//导入押品详细信息-应收租金,无数据
	public static void test31(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("fixed_rent_amount", "amount");//每次收取的固定租金金额（元）
		map.put("rent_oth_desc", "remark");//租赁信息补充说明
		
		
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
	
	//导入押品详细信息-出口退税账户,无数据
	public static void test32(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("special_account_no", "rebate_acc");//专用账户号码
		map.put("exprot_tax_rebate_amount", "amount");//出口退税款金额(元)
		map.put("pledge_property_desc", "remark");//质押财产描述
		
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
	
	//导入押品详细信息-农村电网收费权,无数据
	public static void test33(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("charge_ga_code", "appro_no");//收费权政府批文文号
		map.put("details_address", "net_name");//详细地址
		map.put("fee_scale_desc", "remark");//现行收费标准描述
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* 前面已处理过
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
	
	//导入押品详细信息-其他收费权,无数据
	public static void test34(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("charge_ga_code", "appro_no");//收费权政府批文文号
		map.put("fee_scale_desc", "charge_bz");//现行收费标准描述
		map.put("fee_scale_desc", "remark");//现行收费标准描述
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));


		try {
			/* 前面已处理过
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
	
	//导入押品详细信息-理财
	public static void test35(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("GUAR_NO", "guaranty_id");//押品编号
		map.put("acct_no", "f_account");//账户号码
		map.put("money_prod_code", "f_prd_code");//理财产品代码
		map.put("due_date", "f_end_date");//到期日期
		map.put("pledge_share", "f_amt");//质押份额
		map.put("term", "to_date(f_end_date,'yyyy-mm-dd')-to_date(f_start_date,'yyyy-mm-dd')");//产品期限（天）
		
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
	//导入关键字段1、关键字段2
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
	
	//导入将押品表中有数据，从表中无数据的从表中插入押品编号，如不做此操作将导致无法维护详细信息
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
