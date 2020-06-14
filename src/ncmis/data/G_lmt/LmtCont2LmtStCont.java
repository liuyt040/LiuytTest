package ncmis.data.G_lmt;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import ncmis.data.ConnectPro;
/*
 * 导入同业客户授信协议信息lmt_cont到lmt_st_cont
 * 导入授信台账表 lmt_cont_detail到lmt_st_acc
 * 过滤个人授信
 * 
 */
public class LmtCont2LmtStCont {

	private static List<String> list=null;
	private static Map<String,String> map=null;
	public static void main(String... strings) throws ClassNotFoundException,
			SQLException {
		Class.forName("oracle.jdbc.driver.OracleDriver");
		Connection conn = null;
//		conn = DriverManager.getConnection(
//				"jdbc:oracle:thin:@127.0.0.1:1521:CMIS", "LIUYT", "CMIS");
		conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
		try {
			test1(conn);
			test2(conn);
			test3(conn);
			testOcc(conn);
//			check(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//导入同业授信协议
	public static void test1(Connection conn) {
		//key:新表字段   
		//value:老表字段
		map=new HashMap<String, String>();
		map.put("lmt_cont_no", "lmt_serno");//授信协议号
		map.put("lmt_st_dc_no", "serno");//申请流水号
		map.put("cus_id", "cus_id");//客户编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("same_org_typ", "'01'");//同业机构类型
		map.put("crd_app_typ", "'20'");//授信申请类型
		map.put("crd_type", "'10'");//授信类型：循环
		map.put("lmt_ccy", "'CNY'");//授信业务类型：人民币
		map.put("crd_tot_amt", "crd_totl_sum_amt");//授信协议金额
		map.put("tnr_typ", "decode(term_type,'01','DDD','02','MMM','03','YYY')");//期限类型	
		map.put("lmt_tnr", "term");//期限
		map.put("crd_str_dt", "start_date");//授信起始日期
		map.put("crd_end_dt", "expi_date");//授信到期日期
		map.put("remarks", "remarks");//调查人意见
		//**新系统缺失字段，是否项目授信，担保方式
//		map.put("project_lmt_flag", "decode(prj_lmt_flg,'1','Y','2','N','')");//是否项目授信
//		map.put("GURT_TYP", "assure");//担保方式
		map.put("crt_usr", "cus_manager");//登记人
		map.put("main_usr", "cus_manager");//主管客户经理
		map.put("crt_dt", "update_date");//登记日期
		map.put("lmt_cont_sts", "decode(lmt_state,'00','00','01','01','02','03','03','02','04','02')");//协议状态		
		map.put("instu_cde", "'0000'");//法人编码
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from lmt_st_cont where 1=1  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont@CMISTEST2101 b where length(b.cus_id)>10 )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE lmt_st_cont a SET crt_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=crt_usr) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
						
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE lmt_st_cont a SET main_bch= (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=crt_usr) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_input_br_id!=null){
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//导入对公客户授信台账表，lmt_st_acc 
	public static void test2(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("lmt_st_acc_no", "item_id");//额度台帐编号
		map.put("lmt_cont_no", "lmt_serno");//授信协议编号
		map.put("cus_id", "cus_id");//客户编号
		map.put("cus_name", "cus_name");//客户名称
		map.put("same_org_typ", "'01'");//境内银行
		map.put("crd_typ", "'10'");//循环
		map.put("lmt_ccy", "'CNY'");//币种
		map.put("crd_tot_amt", "crd_lmt");//人工调整授信额度（元）
		map.put("out_amt", "outstnd_lmt");//已用金额（元）
		map.put("avai_amt", "residual_lmt");//可用金额（元）

//		map.put("tnr_typ", "decode(term_type,'01','003','02','002','03','001'");//期限类型
//		map.put("lmt_tnr", "01");//
		
		map.put("crd_str_dt", "start_date");//授信起始日
		map.put("crd_end_dt", "expi_date");//授信到期日
		map.put("lmt_sts", "decode(lmt_state,'00','00','01','01','02','03','03','02','04','02')");//台账状态
		map.put("instu_cde", "'0000'");//法人编码

		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from lmt_st_acc where 1=1 ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_acc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont_details@CMISTEST2101 b where length(b.cus_id)>10  ) ";
			System.out.println(insert_cus_corp);
			
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE lmt_st_acc a SET main_usr= (SELECT  main_usr FROM lmt_st_cont b WHERE a.LMT_CONT_NO=b.LMT_CONT_NO) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			
			PreparedStatement up_input_br_id=null;
			String input_br_id=" UPDATE lmt_st_acc a SET main_bch = (SELECT  b.usr_bch FROM s_usr b WHERE b.usr_cde=a.main_usr) ";
			up_input_br_id=conn.prepareStatement(input_br_id);
			up_input_br_id.execute();
			
			
			
			//将授信协议预登记状态改为签订
			String up_lmt_cont_sts = " UPDATE lmt_st_cont a SET a.lmt_cont_sts='01'  WHERE a.lmt_cont_sts='00' ";
			PreparedStatement up_lmt_cont_sts_ps=null;
			System.out.println(up_lmt_cont_sts);
			up_lmt_cont_sts_ps=conn.prepareStatement(up_lmt_cont_sts);
			up_lmt_cont_sts_ps.execute();
			if(up_lmt_cont_sts_ps!=null){
				up_lmt_cont_sts_ps.close();
			}
			
			//将授信台账预登记状态改为签订
			String up_acc_sts = " UPDATE Lmt_st_Acc a SET a.lmt_sts='01'  WHERE a.lmt_sts='00' ";
			PreparedStatement up_acc_sts_ps=null;
			System.out.println(up_acc_sts);
			up_acc_sts_ps=conn.prepareStatement(up_acc_sts);
			up_acc_sts_ps.execute();
			if(up_acc_sts_ps!=null){
				up_acc_sts_ps.close();
			}
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_input_br_id!=null){
				up_input_br_id.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	
	//导入同业客户授信批复表，lmt_st_dc 
	public static void test3(Connection conn) {
		//key:新表字段   
		//value:老表字段
		//新系统缺少注册地行政区划reg_state_code，和行政区划名称reg_area_name
		map=new HashMap<String, String>();
		map.put("LMT_ST_DC_NO", "serno");//批复号
		map.put("lmt_st_no", "serno");//申请流水号
		map.put("cus_id", "cus_id");//客户编号
		map.put("large_bank_no", "cus_id");//行号
		map.put("same_org_sub_typ", "'110'");//同业机构子类型
		map.put("cus_name", "cus_name");//客户名称
		map.put("crd_app_typ", "'20'");//单独授信
		map.put("lmt_ccy", "'CNY'");//币种
		map.put("crd_tot_amt", "crd_lmt");//人工调整授信额度（元）
		map.put("instu_cde", "'0000'");//法人编码

		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table lmt_st_dc ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO lmt_st_dc ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM lmt_cont@CMISTEST2101 where length(cus_id)>10 ) ";
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
	
	//导入授信台账占用明细表，limit_occupy 
	/*按顺序移植
	 * 同业授信明细
	 * 同业客户授信明细
	 */
	//以下代码移植【同业授信】
	public static void testOcc(Connection conn) {
		/*同业的占用异常数据查询，筛选出同一合同有多条占用记录的数据
		 * SELECT cont_no,COUNT(*) FROM (SELECT * FROM lmt_journal_acc a WHERE a.details_type='001' AND length(a.cus_id)=10) GROUP BY cont_no  HAVING COUNT(*)>1;
		 */
		String de_cont=" DELETE FROM cont ";
		String in_loan=" INSERT INTO cont SELECT a.cont_no FROM ctr_disc_cont@CMISTEST2101 a WHERE a.cont_state='200' ";
		
		String de_occ=" DELETE FROM limit_occupy  a where a.occ_business_seq like '%TXHT%' ";
		String occ_001=" INSERT INTO limit_occupy(ID,limit_id,limit_item_id,occ_business_seq,occ_amt,occ_stage,state,cus_id,currency,create_time,lmt_blc,last_chg_dt,Instu_Cde,limit_type,crd_typ) " 
						+" SELECT '001'||b.cont_no||b.item_id,'',item_id,cont_no,outstnd_lmt,'1','1','','CNY','',outstnd_lmt,'','0000','1','20' FROM lmt_cont_rel_bank@CMISTEST2101 b WHERE b.cont_no IN ( SELECT cont_no FROM cont ) AND B.OUTSTND_LMT>0 ";//占用
		String occ_002="INSERT INTO limit_occupy(ID,limit_id,limit_item_id,occ_business_seq,occ_amt,occ_stage,state,cus_id,currency,create_time,lmt_blc,last_chg_dt,Instu_Cde,limit_type,crd_typ) " 
						+" SELECT '002'||b.cont_no||b.item_id,'',item_id,cont_no,restored_lmt,'1','4','','CNY','',outstnd_lmt-restored_lmt,'','0000','1','20' FROM lmt_cont_rel_bank@CMISTEST2101 b WHERE b.cont_no IN ( SELECT cont_no FROM cont ) AND B.OUTSTND_LMT>0 ";//恢复
		//删除发生额为0的数据
		String de_occ_0=" DELETE FROM limit_occupy where occ_amt=0 ";
		
		try {
			PreparedStatement de_cont_ps=null;
			de_cont_ps=conn.prepareStatement(de_cont);
			de_cont_ps.execute();
			
			PreparedStatement in_loan_ps=null;
			System.out.println(in_loan);
			in_loan_ps=conn.prepareStatement(in_loan);
			in_loan_ps.execute();
			
			PreparedStatement de_occ_ps=null;
			System.out.println(de_occ);
			de_occ_ps=conn.prepareStatement(de_occ);
			de_occ_ps.execute();
			
			PreparedStatement occ_001_ps=null;
			System.out.println(occ_001);
			occ_001_ps=conn.prepareStatement(occ_001);
			occ_001_ps.execute();
			
			PreparedStatement occ_002_ps=null;
			System.out.println(occ_002);
			occ_002_ps=conn.prepareStatement(occ_002);
			occ_002_ps.execute();
			
			PreparedStatement de_occ_0_ps=null;
			System.out.println(de_occ_0);
			de_occ_0_ps=conn.prepareStatement(de_occ_0);
			de_occ_0_ps.execute();
			
			if(de_cont_ps!=null){
				de_cont_ps.close();
			}
			if(in_loan_ps!=null){
				in_loan_ps.close();
			}
			if(de_occ_ps!=null){
				de_occ_ps.close();
			}
			if(occ_001_ps!=null){
				occ_001_ps.close();
			}
			if(occ_002_ps!=null){
				occ_002_ps.close();
			}
			if(de_occ_0_ps!=null){
				de_occ_0_ps.close();
			}
			/*筛选占用明细与恢复明细时间相同的数据，会影像后续业务
			 *1 SELECT a.occ_business_seq,a.create_time,b.create_time FROM (SELECT * FROM limit_occupy a WHERE a.state='1' ) a
JOIN (SELECT * FROM limit_occupy a WHERE a.state='4' ) b
ON a.occ_business_seq=b.occ_business_seq WHERE a.create_time=b.create_time
			 * 2 查看创建时间为空的数据
			 */
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	public static void check(Connection conn){
		//校验授信协议和授信台账金额是否一致
		String check_amt=" SELECT a.lmt_cont_no,b.lmt_cont_no,a.crd_tot_amt,b.crd_tot_amt FROM lmt_st_cont a "
						+" LEFT JOIN lmt_st_acc b ON a.lmt_cont_no=b.lmt_cont_no "
						+" WHERE a.crd_tot_amt!=b.crd_tot_amt ";
		
		//校验占用明细余额与授信台账余额不一致的数据
		String check_out=" SELECT lmt_st_acc_no,e.out_amt,f.* FROM lmt_st_acc e " 
						+" LEFT JOIN ( "
						+" SELECT c.*,d.occ_amt_2,c.occ_amt_1-decode(d.occ_amt_2,NULL,0,d.occ_amt_2) out_amt1  FROM ( "
						+" SELECT a.limit_item_id,a.state,SUM(a.occ_amt) occ_amt_1 FROM limit_occupy a WHERE a.state='1' GROUP BY a.limit_item_id,a.state " 
						+" ) c LEFT JOIN (SELECT b.limit_item_id,b.state,SUM(b.occ_amt) occ_amt_2 FROM limit_occupy b WHERE b.state='4' GROUP BY b.limit_item_id,b.state ) d ON c.limit_item_id = d.limit_item_id "
						+" ) f ON lmt_st_acc_no=f.limit_item_id WHERE out_amt!=out_amt1" ;
		
		//检查同一笔业务，占用时间与恢复时间相同的数据，会影响后续额度恢复
		String check_create_time="SELECT a.limit_item_id,a.occ_business_seq,a.create_time,b.create_time FROM (SELECT * FROM limit_occupy a WHERE a.state='1' ) a "
								+" JOIN (SELECT * FROM limit_occupy a WHERE a.state='4' ) b "
								+" ON a.occ_business_seq=b.occ_business_seq and a.limit_item_id=b.limit_item_id WHERE a.create_time=b.create_time";
		
		PreparedStatement check_amt_ps=null;
		PreparedStatement check_out_ps=null;
		PreparedStatement check_create_time_ps=null;
		try {
			check_amt_ps=conn.prepareStatement(check_amt);
			ResultSet rs=check_amt_ps.executeQuery();
			System.out.println("*********开始校验：校验同业客户授信协议与授信台账总金额不符的数据*********");
			while(rs.next()){
				System.out.println(rs.getObject(1)+" "+rs.getObject(2)+" "+rs.getObject(3)+" "+rs.getObject(4));
			}
			System.out.println("*********结束校验：校验同业客户授信协议与授信台账总金额不符的数据*********");
			if(rs!=null){
				rs.close();
			}
			
			check_out_ps=conn.prepareStatement(check_out);
			ResultSet rs1=check_out_ps.executeQuery();
			System.out.println("*********开始校验：校验同业客户占用明细占用金额与授信台账占用金额不符的数据*********");
			while(rs1.next()){
				System.out.println("授信台账："+rs1.getObject(1)+"，台账占用金额： "+rs1.getObject(2)+"， 明细统计的占用金额："+rs1.getObject(7));
			}
			System.out.println("*********结束校验：校验同业客户占用明细占用金额与授信台账占用金额不符的数据*********");
			if(rs1!=null){
				rs1.close();
			}
			
			check_create_time_ps=conn.prepareStatement(check_create_time);
			ResultSet rs2=check_create_time_ps.executeQuery();
			System.out.println("*********开始校验：校验同业客户授信使用明细表中同一笔数据占用记录与恢复记录创建时间相同的数据*********");
			while(rs2.next()){
				System.out.println("台账编号："+rs2.getObject(1)+"业务编号："+rs2.getObject(2)+"，创建时间： "+rs2.getObject(3)+"，恢复时间 "+rs2.getObject(4));
			}
			System.out.println("*********结束校验：：校验同业客户授信使用明细表中同一笔数据占用记录与恢复记录创建时间相同的数据*********");
			if(rs2!=null){
				rs2.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
			if(check_amt_ps!=null){
				try {
					check_amt_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(check_out_ps!=null){
				try {
					check_out_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(check_create_time_ps!=null){
				try {
					check_create_time_ps.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
	}

}
