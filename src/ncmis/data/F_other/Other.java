package ncmis.data.F_other;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import ncmis.data.ConnectPro;

public class Other {

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

	//导入贷款审批历史
	public static void test1(Connection conn) {

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table wfi_oldcmis  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			//贷款申请审批
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_loan_cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			//贷款出账审批
			PreparedStatement insert_pvp_loan_ps=null;
			String insert_pvp_loan="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_loan_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_loan);
			insert_pvp_loan_ps=conn.prepareStatement(insert_pvp_loan);
			insert_pvp_loan_ps.execute();
			if(insert_pvp_loan_ps!=null){
				insert_pvp_loan_ps.close();
			}
			//展期申请
			PreparedStatement insert_iqp_extend_ps=null;
			String insert_iqp_extend="INSERT INTO  wfi_oldcmis "
									+" SELECT o_cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_loan_extend@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_extend);
			insert_iqp_extend_ps=conn.prepareStatement(insert_iqp_extend);
			insert_iqp_extend_ps.execute();
			if(insert_iqp_extend_ps!=null){
				insert_iqp_extend_ps.close();
			}
			//展期出账审批
			PreparedStatement insert_pvp_extend_ps=null;
			String insert_pvp_extend="INSERT INTO  wfi_oldcmis "
									+" SELECT c.o_cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_loan_extend@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_extend);
			insert_pvp_extend_ps=conn.prepareStatement(insert_pvp_extend);
			insert_pvp_extend_ps.execute();
			if(insert_pvp_extend_ps!=null){
				insert_pvp_extend_ps.close();
			}
			//缩期业务申请
			PreparedStatement insert_iqp_shirk_ps=null;
			String insert_iqp_shirk="INSERT INTO  wfi_oldcmis "
									+" SELECT o_cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN Ctr_Loan_Shrink@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_shirk);
			insert_iqp_shirk_ps=conn.prepareStatement(insert_iqp_shirk);
			insert_iqp_shirk_ps.execute();
			if(insert_iqp_shirk_ps!=null){
				insert_iqp_shirk_ps.close();
			}
			//缩期出账审批
			PreparedStatement insert_pvp_shirk_ps=null;
			String insert_pvp_shirk="INSERT INTO  wfi_oldcmis "
									+" SELECT c.o_cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_loan_Shrink@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_shirk);
			insert_pvp_shirk_ps=conn.prepareStatement(insert_pvp_shirk);
			insert_pvp_shirk_ps.execute();
			if(insert_pvp_shirk_ps!=null){
				insert_pvp_shirk_ps.close();
			}
			//担保合同解除
			PreparedStatement insert_Grt_Guar_Cont_Remove_ps=null;
			String insert_Grt_Guar_Cont_Remove="INSERT INTO  wfi_oldcmis "
									+" SELECT guar_cont_no,c.ser_no,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN Grt_Guar_Cont_Remove@CMISTEST2101 c ON  c.ser_no=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_Grt_Guar_Cont_Remove);
			insert_Grt_Guar_Cont_Remove_ps=conn.prepareStatement(insert_Grt_Guar_Cont_Remove);
			insert_Grt_Guar_Cont_Remove_ps.execute();
			if(insert_Grt_Guar_Cont_Remove_ps!=null){
				insert_Grt_Guar_Cont_Remove_ps.close();
			}
			//担保合同新增与变更
			PreparedStatement insert_Grt_Change_Cont_ps=null;
			String insert_Grt_Change_Cont="INSERT INTO  wfi_oldcmis "
									+" SELECT c.guar_cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN Grt_Change_Cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_Grt_Change_Cont);
			insert_Grt_Change_Cont_ps=conn.prepareStatement(insert_Grt_Change_Cont);
			insert_Grt_Change_Cont_ps.execute();
			if(insert_Grt_Change_Cont_ps!=null){
				insert_Grt_Change_Cont_ps.close();
			}
			//利率调整
			PreparedStatement insert_Iqp_Loan_Rate_ps=null;
			String insert_Iqp_Loan_Rate="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN Iqp_Loan_Rate@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_Iqp_Loan_Rate);
			insert_Iqp_Loan_Rate_ps=conn.prepareStatement(insert_Iqp_Loan_Rate);
			insert_Iqp_Loan_Rate_ps.execute();
			if(insert_Iqp_Loan_Rate_ps!=null){
				insert_Iqp_Loan_Rate_ps.close();
			}
			//银承业务申请
			PreparedStatement insert_iqp_accp_ps=null;
			String insert_iqp_accp="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_accp_cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_accp);
			insert_iqp_accp_ps=conn.prepareStatement(insert_iqp_accp);
			insert_iqp_accp_ps.execute();
			if(insert_iqp_accp_ps!=null){
				insert_iqp_accp_ps.close();
			}
			//银承出账审批
			PreparedStatement insert_pvp_accp_ps=null;
			String insert_pvp_accp="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_accp_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_accp);
			insert_pvp_accp_ps=conn.prepareStatement(insert_pvp_accp);
			insert_pvp_accp_ps.execute();
			if(insert_pvp_accp_ps!=null){
				insert_pvp_accp_ps.close();
			}
			//保函业务申请
			PreparedStatement insert_iqp_cvrg_ps=null;
			String insert_iqp_cvrg="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_cvrg_cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_cvrg);
			insert_iqp_cvrg_ps=conn.prepareStatement(insert_iqp_cvrg);
			insert_iqp_cvrg_ps.execute();
			if(insert_iqp_cvrg_ps!=null){
				insert_iqp_cvrg_ps.close();
			}
			//保函出账审批
			PreparedStatement insert_pvp_cvrg_ps=null;
			String insert_pvp_cvrg="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_cvrg_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_cvrg);
			insert_pvp_cvrg_ps=conn.prepareStatement(insert_pvp_cvrg);
			insert_pvp_cvrg_ps.execute();
			if(insert_pvp_cvrg_ps!=null){
				insert_pvp_cvrg_ps.close();
			}
			//信用证
			PreparedStatement insert_iqp_itsmstin_ps=null;
			String insert_iqp_itsmstin="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_itsmstin_cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_itsmstin);
			insert_iqp_itsmstin_ps=conn.prepareStatement(insert_iqp_itsmstin);
			insert_iqp_itsmstin_ps.execute();
			if(insert_iqp_itsmstin_ps!=null){
				insert_iqp_itsmstin_ps.close();
			}
			//信用证出账审批
			PreparedStatement insert_pvp_itsmstin_ps=null;
			String insert_pvp_itsmstin="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_itsmstin_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_itsmstin);
			insert_pvp_itsmstin_ps=conn.prepareStatement(insert_pvp_itsmstin);
			insert_pvp_itsmstin_ps.execute();
			if(insert_pvp_itsmstin_ps!=null){
				insert_pvp_itsmstin_ps.close();
			}
			
			//贸易融资业务申请
			PreparedStatement insert_iqp_itsmstout_ps=null;
			String insert_iqp_itsmstout="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN ctr_itsmstin_cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_itsmstout);
			insert_iqp_itsmstout_ps=conn.prepareStatement(insert_iqp_itsmstout);
			insert_iqp_itsmstout_ps.execute();
			if(insert_iqp_itsmstout_ps!=null){
				insert_iqp_itsmstout_ps.close();
			}
			//贸易融资出账审批
			PreparedStatement insert_pvp_itsmstout_ps=null;
			String insert_pvp_itsmstout="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_itsmstin_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_itsmstout);
			insert_pvp_itsmstout_ps=conn.prepareStatement(insert_pvp_itsmstout);
			insert_pvp_itsmstout_ps.execute();
			if(insert_pvp_itsmstout_ps!=null){
				insert_pvp_itsmstout_ps.close();
			}
			
			//贴现业务申请
			PreparedStatement insert_iqp_disc_ps=null;
			String insert_iqp_disc="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN Ctr_Disc_Cont@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_iqp_disc);
			insert_iqp_disc_ps=conn.prepareStatement(insert_iqp_disc);
			insert_iqp_disc_ps.execute();
			if(insert_iqp_disc_ps!=null){
				insert_iqp_disc_ps.close();
			}
			//贴现出账审批
			PreparedStatement insert_pvp_disc_ps=null;
			String insert_pvp_disc="INSERT INTO  wfi_oldcmis "
									+" SELECT c.cont_no,c.serno,a.* FROM wfi_app_advice@CMISTEST2101 a "
									+" JOIN WFI_JOIN@CMISTEST2101 b ON b.instanceid=a.wfi_instance_id "
									+" JOIN pvp_disc_app@CMISTEST2101 c ON  c.serno=b.pk_value "
									+" ORDER BY wfi_instance_id,operate_time  ";
			System.out.println(insert_pvp_disc);
			insert_pvp_disc_ps=conn.prepareStatement(insert_pvp_disc);
			insert_pvp_disc_ps.execute();
			if(insert_pvp_disc_ps!=null){
				insert_pvp_disc_ps.close();
			}

			//将审批结果由码值转为描述
			PreparedStatement up_1_ps=null;
			String up_1=" UPDATE wfi_oldcmis b SET b.app_conclusion = ( SELECT a.cnname FROM s_dic@CMISTEST2101 a WHERE a.opttype='SCSPJL' AND a.enname=b.app_conclusion  )";
			System.out.println(up_1);
			up_1_ps=conn.prepareStatement(up_1);
			up_1_ps.execute();
			if(up_1_ps!=null){
				up_1_ps.close();
			}
			//将申请状态改为描述
			PreparedStatement up_2_ps=null;
			String up_2=" UPDATE wfi_oldcmis b SET b.wfi_status = ( SELECT a.cnname FROM s_dic@CMISTEST2101 a WHERE a.opttype='STD_ZB_APPR_STATUS' AND a.enname=b.wfi_status  )";
			System.out.println(up_2);
			up_2_ps=conn.prepareStatement(up_2);
			up_2_ps.execute();
			if(up_2_ps!=null){
				up_2_ps.close();
			}
			//将流程标识改为流程名称
			PreparedStatement up_3_ps=null;
			String up_3=" UPDATE wfi_oldcmis b SET b.wfsign = ( SELECT a.wfname FROM wf_studio@CMISTEST2101 a WHERE a.wfsign=b.wfsign AND A.STATE='R'  )";
			System.out.println(up_3);
			up_3_ps=conn.prepareStatement(up_3);
			up_3_ps.execute();
			if(up_3_ps!=null){
				up_3_ps.close();
			}


			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
}
