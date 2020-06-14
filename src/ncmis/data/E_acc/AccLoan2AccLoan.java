package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class AccLoan2AccLoan {
	
//	private static List<String> list=null;
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
//			test2(conn);
//			test3(conn);
//			test4(conn);
//			test5(conn);
//			test6(conn);
//			test7(conn);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}

	//�������̨�ˣ�
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//̨��״̬,���ݺ���״̬���޸�����������״̬
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
		map.put("prd_cde", "decode(biz_type,'000015','01029340','010169','01021315','010006','01027580','000027','01026960','000058','01026960','000058','01026960','010273','01026960','010881','01022205','000028','01025910','010374','01026960','010171','01022100','010172','01022100','010173','01022100','000121','01011040','021180','01018946','021060','01018319','000004','01012974','000005','01015330','000017','01012606','000052','01017030','005040','01014350','010005','01011522','010005','01011522','022179','01012731','000006','01017162','000018','01011864','000019','01014244','000020','01013896','000051','01018133','010004','01014172','010007','01014921','010008','01011589','010009','01014730','021061','01017856','022178','01017162','022184','01017863','000091','01023134',biz_type)");//��Ʒ���,��������ѭ����Ʒ�Ͳ�ѭ����Ʒ
		map.put("cur_type", "cur_type");//�������
		map.put("loan_amount", "loan_amount");//�����Ԫ)
		map.put("loan_start_date", "loan_start_date");//�������
		map.put("loan_end_date", "loan_end_date");//������ֹ��
		map.put("loan_end_date_old", "orig_expi_date");//ԭ��������
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
		map.put("flt_rt_pc", "floating_rate");//��������
		map.put("reality_ir_y", "reality_ir_y");//ִ������(��)
		map.put("ovr_ir_y", "overdue_ir*12");//���ڷ�Ϣ����(�꣩
		map.put("flt_ovr_rt", "overdue_rate");//���ڼӷ�����
		map.put("flt_dfl_rt", "default_rate");//ΥԼ�ӷ�����
		map.put("dfl_ir_y", "default_ir*12");//ΥԼ��Ϣ����(��)
		map.put("assure_means_main", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("assure_means2", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("assure_means3", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//���ʽ
		map.put("loan_balance", "loan_balance");//�������(Ԫ)
		map.put("revolving_times", "''");//���»��ɴ���
		map.put("extension_times", "extension_times");//չ�ڴ���
		map.put("cla", "cla");//�弶����
		//ȡ��ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����
		map1.put("dirt_name", "direction_name");//����Ͷ������
		map1.put("agr_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map1.put("serno", "serno");//ҵ����ˮ��
		map1.put("base_rt_knd", "decode(rate_type,'2','30',10)");//��������
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map1.put("fix_int_sprd", "lpr_natu_base_point");//����������
		map1.put("ruling_ir", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��������
		
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("cus_manager", "cus_manager");//������
		map.put("main_br_id", "input_br_id");//���ܻ���
		map.put("iqp_dt", "latest_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
//		map.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//Э��״̬
//		map.put("wf_appr_sts", "'997'");//����״̬��ȫ��Ϊͨ��
		
		map.put("instu_cde", "'0000'");//���˱���
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table acc_loan   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type not in ('010881','022184','010882') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			//���ݺ�ͬ��Ϣ���½����Ϣ
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE acc_loan a SET ("+keys1.toString().substring(1)+") = ( SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			
			/*
			//���¼���չ�ڴ����ͽ��»��ɴ���
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_loan a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM acc_loan@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			*/
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE acc_loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//���¸���֤�����͡�֤������
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE acc_loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_loan_cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_loan b  )";
			System.out.println(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps=conn.prepareStatement(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps.execute();
			if(delte_ctr_loan_cont_ps!=null){
				delte_ctr_loan_cont_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ί�д���̨�ˣ�acc_loan 2 Acc_Corp_Loan
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//̨��״̬,���ݺ���״̬���޸�����������״̬
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("bill_no", "bill_no");//��ͬ���
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
//		map.put("base_rt_knd", "'10'");//��������
		map.put("prd_cde", "decode(biz_type,'022184','01017863','010881','01022205')");
		map.put("cur_type", "cur_type");//�������
		map.put("loan_amount", "loan_amount");//�����Ԫ)
		map.put("loan_start_date", "loan_start_date");//�������
		map.put("loan_end_date", "loan_end_date");//������ֹ��
		map.put("loan_end_date_old", "orig_expi_date");//ԭ��������,
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
		map.put("floating_pct", "floating_rate");//��������
		map.put("rel_ir_y", "reality_ir_y");//ִ������(��)
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("ovr_ir_y", "overdue_ir*12");//���ڷ�Ϣ����(�꣩
		map.put("flt_ovr_rt", "overdue_rate");//���ڼӷ�����
		map.put("flt_dfl_rt", "default_rate");//ΥԼ�ӷ�����
		map.put("dfl_ir_y", "default_ir*12");//ΥԼ��Ϣ����(��)
		map.put("assure_means_main", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("assure_means2", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("assure_means3", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//���ʽ
		map.put("loan_balance", "loan_balance");//�������(Ԫ)
		map.put("revolving_times", "''");//���»��ɴ���
		map.put("extension_times", "extension_times");//չ�ڴ���
		map.put("cla", "cla");//�弶����
		//ȡ��ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����
		map1.put("dirt_name", "direction_name");//����Ͷ������
		map1.put("agr_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map1.put("serno", "serno");//ҵ����ˮ��
		map1.put("cont_typ", "decode(cont_type,'2','02','01')");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
		map1.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map1.put("usg_dsc", "use_dec");//��;����
		map1.put("base_rt_knd", "decode(rate_type,'1','10','2','30')");//��������
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map1.put("fix_int_sprd", "lpr_natu_base_point");//����������
		map1.put("ruling_ir", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��׼����
		
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("cus_manager", "cus_manager");//������
		map.put("main_br_id", "input_br_id");//���ܻ���
//		map.put("iqp_dt", "latest_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
//		map.put("cont_sts", "decode(cont_state,'100','02','200','09','300','42','999','90')");//Э��״̬
//		map.put("wf_appr_sts", "'997'");//����״̬��ȫ��Ϊͨ��
		
		map.put("instu_cde", "'0000'");//���˱���
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys1.toString().substring(1));
		System.out.println(values1.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table Acc_Corp_Loan  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Acc_Corp_Loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type in ('010881','022184') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			//���ݺ�ͬ��Ϣ���½����Ϣ
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE Acc_Corp_Loan a SET ("+keys1.toString().substring(1)+") = ( SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			
			/*
			//���¼���չ�ڴ����ͽ��»��ɴ���
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_loan a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM acc_loan@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			*/
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Acc_Corp_Loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//���¸���֤�����͡�֤������
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM ctr_corp_loan_entr a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM Acc_Corp_Loan b  )";
			System.out.println(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps=conn.prepareStatement(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps.execute();
			if(delte_ctr_loan_cont_ps!=null){
				delte_ctr_loan_cont_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���뱣��̨������
	public static void test3(Connection conn) {

		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		map=new HashMap<String, String>();
		map.put("account_status", "decode(account_status,'0','1','1','2','9','4')");//̨��״̬,���ݺ���״̬���޸�����������״̬
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("bill_no", "bill_no");//��ݱ��
		map.put("cus_name", "cus_name");//�ͻ�����
		map.put("cus_id", "cus_id");//�ͻ����
//		map.put("base_rt_knd", "'10'");//��������
		map.put("prd_cde", "'01029525'");//��Ʒ���
		map.put("loan_amount", "loan_amount");//�����Ԫ)
		map.put("cur_type", "cur_type");//�������
		map.put("loan_start_date", "loan_start_date");//�������
		map.put("loan_end_date", "loan_end_date");//������ֹ��
		map.put("loan_end_date_old", "orig_expi_date");//ԭ��������
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
		map.put("flt_rt_pct", "floating_rate");//��������
		map.put("rel_ir_y", "reality_ir_y");//ִ������(��)
		map.put("ovr_ir_y", "overdue_ir*12");//���ڷ�Ϣ����(�꣩
		map.put("flt_ovr_rt", "overdue_rate");//���ڼӷ�����
		map.put("flt_dfl_rt", "default_rate");//ΥԼ�ӷ�����
		map.put("dfl_ir_y", "default_ir*12");//ΥԼ��Ϣ����(��)
		map.put("repayment_mode", "decode(repayment_mode,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31')");//���ʽ
		map.put("loan_balance", "loan_balance");//�������(Ԫ)
		map.put("revolving_times", "''");//���»��ɴ���
		map.put("extension_times", "extension_times");//չ�ڴ���
		map.put("cla", "cla");//�弶����
		//��ͬ
		Map<String,String> map1 = new HashMap<String,String>();
		map1.put("serno", "serno");//ҵ����ˮ��
		map1.put("cycle_ind", "decode(cont_type,'2','Y','N')");//�Ƿ�ѭ��
		map1.put("limit_ind", "limit_ind");//���ŷ�ʽ
		map1.put("lmt_cont_no", "limit_ma_no");//����Э����
		map1.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map1.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map1.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map1.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		map1.put("serno", "serno");//ҵ����ˮ��
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map1.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
		map1.put("use_dec", "use_dec");//��;����
		map1.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map1.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map1.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map1.put("dirt_cde", "loan_direction");//����Ͷ�����
		map1.put("dirt_name", "direction_name");//����Ͷ������
		map1.put("agr_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map1.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
		map1.put("use_dec", "use_dec");//��;����
		map1.put("base_rt_knd", "decode(rate_type,'2','30',10)");//��������
		map1.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map1.put("fix_int_sprd", "lpr_natu_base_point");//����������
		map1.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��׼����
		
		map.put("opr_usr", "cus_manager");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cus_manager");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cus_manager");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cus_manager");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "loan_start_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		
		map.put("instu_cde", "'0000'");//���˱���
		
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		StringBuffer keys1=new StringBuffer("");
		StringBuffer values1=new StringBuffer("");
		for (Map.Entry<String, String> entry1 : map1.entrySet()) { 
			keys1.append(","+entry1.getKey());
			values1.append(","+entry1.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" truncate table acc_fact ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO acc_fact ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM acc_loan@CMISTEST2101 b where b.biz_type  = '010882' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			PreparedStatement up_acc_fact_ps=null;
			String up_acc_fact=" update acc_fact a set ("+keys1.toString().substring(1)+")  = (SELECT "+values1.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where  b.cont_no=a.cont_no )";
			System.out.println(up_acc_fact);
			up_acc_fact_ps=conn.prepareStatement(up_acc_fact);
			up_acc_fact_ps.execute();
			
			//�����Ƿ�ͷ��գ��Թ�ʹ������Ϊ�߷��գ�����Ϊ�ͷ���
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE acc_fact a SET a.lr_ind = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE acc_fact a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE acc_fact a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			
			//ɾ��û��̨�˵ĺ�ͬ��δ�����ϵģ���;�ģ�
			PreparedStatement delte_ctr_loan_cont_ps=null;
			String delte_ctr_loan_cont=" DELETE FROM Ctr_Fact_Cont a WHERE a.cont_no NOT IN ( SELECT distinct cont_no FROM acc_fact b  )";
			System.out.println(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps=conn.prepareStatement(delte_ctr_loan_cont);
			delte_ctr_loan_cont_ps.execute();
			if(delte_ctr_loan_cont_ps!=null){
				delte_ctr_loan_cont_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
	}
	
	//���ݺ������ݿ������ͨ����������Ϣ
	public static void test4(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		try {
			//���ݺ������ݿ����������Ϣ
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//�������
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//Ӧ��������Ϣ
			map2.put("inner_actl_int", "SETL_NORM_INT");//ʵ��������Ϣ
			map2.put("overdue_actl_int", "ps_setl_int");//��ǰǷ������Ϣ 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//��ǰǷ������Ϣ
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//Ӧ�շ�Ϣ
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//ʵ�շ�Ϣ
			map2.put("delay_int_cumu", "ps_setl_od_int");//Ƿ���ڷ�Ϣ
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//Ƿ���ⷣϢ
			map2.put("inner_int_cumu", "accrued_Int_Nor");//��ǰ������Ϣ
			map2.put("off_int_cumu", "accrued_Int_Pen");//��ǰ���ᷣϢ
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//��������
			map2.put("od_days", "od_days");//ǷϢ����
			map2.put("od_prcp_days", "od_prcp_days");//Ƿ������
			map2.put("overdue_balance", "od_amt");//���ڱ���
			map2.put("over_times_current", "ps_od_num");//��ǰ��������
			map2.put("over_times_total", "ps_od_sum");//�ۼ���������
			map2.put("latest_repay_date", "cur_due_dt");//��һ�λ�����
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//��������״̬
			map2.put("is_int", "stop_ind");//�Ƿ�ͣϢ��־
			map2.put("stpint_dt", "stop_date");//ͣϢ����
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//��ʸ��£�������ͼ����
			String ycloans = " SELECT BILL_NO FROM acc_loan a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_loan a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				System.out.println(low_risk);
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
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
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			
			//�������ں����������Ϊ����
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_loan a SET a.account_status='4',a.loan_debt_sts='SETL' WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//���ݺ�����½�ݵĻ����� due_day
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_loan a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_loan a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_loan_cont a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no GROUP BY loan_cont_no,loan_rate_mode  ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			//��������Ϊ��������ʱ�����ʵ�����ʽΪ��
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_loan a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//���ݺ�����»��ʽΪ�յ�����
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_loan a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//����������������������ΪǷϢ������Ƿ�������е����ֵ
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_loan a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���ݺ������ݿ����ί�д���������Ϣ
	public static void test5(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		try {
			//���ݺ������ݿ����������Ϣ
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//�������
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//Ӧ��������Ϣ
			map2.put("inner_actl_int", "SETL_NORM_INT");//ʵ��������Ϣ
			map2.put("overdue_actl_int", "ps_setl_int");//��ǰǷ������Ϣ 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//��ǰǷ������Ϣ
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//Ӧ�շ�Ϣ
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//ʵ�շ�Ϣ
			map2.put("delay_int_cumu", "ps_setl_od_int");//Ƿ���ڷ�Ϣ
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//Ƿ���ⷣϢ
			map2.put("inner_int_cumu", "accrued_Int_Nor");//��ǰ������Ϣ
			map2.put("off_int_cumu", "accrued_Int_Pen");//��ǰ���ᷣϢ
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//��������
			map2.put("od_days", "od_days");//ǷϢ����
			map2.put("od_prcp_days", "od_prcp_days");//Ƿ������
			map2.put("overdue_balance", "od_amt");//���ڱ���
			map2.put("over_times_current", "ps_od_num");//��ǰ��������
			map2.put("over_times_total", "ps_od_sum");//�ۼ���������
			map2.put("latest_repay_date", "cur_due_dt");//��һ�λ�����
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//��������״̬
			map2.put("is_int", "stop_ind");//�Ƿ�ͣϢ��־
			map2.put("stpint_dt", "stop_date");//ͣϢ����
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//��ʸ��£�������ͼ����
			String ycloans = " SELECT BILL_NO FROM acc_corp_loan a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_corp_loan a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
				
				
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			//�����㲻���ڵ�������Ϊ����
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_corp_loan a SET a.account_status='4',a.loan_debt_sts='SETL'  WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//���½�ݵĻ�����
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_corp_loan a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  ) ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_corp_loan a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_corp_loan_entr a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no  GROUP BY loan_cont_no,loan_rate_mode ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			
			//��������Ϊ��������ʱ�����ʵ�����ʽΪ��
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_corp_loan a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//���ݺ�����»��ʽΪ�յ�����
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_corp_loan a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//����������������������ΪǷϢ������Ƿ�������е����ֵ
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_corp_loan a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}


	//���ݺ������ݿ���±���������Ϣ
	public static void test6(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		try {
			//���ݺ������ݿ����������Ϣ
			Map<String,String> map2 = new HashMap<String,String>();
			map2.put("loan_balance", "loan_os_prcp");//�������
			map2.put("inner_rece_int", "PS_NORM_INT_AMT");//Ӧ��������Ϣ
			map2.put("inner_actl_int", "SETL_NORM_INT");//ʵ��������Ϣ
			map2.put("overdue_actl_int", "ps_setl_int");//��ǰǷ������Ϣ 
			map2.put("ps_bw_setl_int", "ps_bw_setl_int");//��ǰǷ������Ϣ
			map2.put("rece_int_cumu", "PS_OD_INT_AMT");//Ӧ�շ�Ϣ
			map2.put("actual_int_cumu", "SETL_OD_INT_AMT");//ʵ�շ�Ϣ
			map2.put("delay_int_cumu", "ps_setl_od_int");//Ƿ���ڷ�Ϣ
			map2.put("ps_bw_setl_od_int", "ps_bw_setl_od_int");//Ƿ���ⷣϢ
			map2.put("inner_int_cumu", "accrued_Int_Nor");//��ǰ������Ϣ
			map2.put("off_int_cumu", "accrued_Int_Pen");//��ǰ���ᷣϢ
//			map2.put("overdue_rece_int", "decode(od_days>od_prcp_days,od_days,od_prcp_days)");//��������
			map2.put("od_days", "od_days");//ǷϢ����
			map2.put("od_prcp_days", "od_prcp_days");//Ƿ������
			map2.put("overdue_balance", "od_amt");//���ڱ���
			map2.put("over_times_current", "ps_od_num");//��ǰ��������
			map2.put("over_times_total", "ps_od_sum");//�ۼ���������
			map2.put("latest_repay_date", "cur_due_dt");//��һ�λ�����
			map2.put("loan_debt_sts", "decode(loan_debt_sts,'CHRGO','OFFED',loan_debt_sts)");//��������״̬
			map2.put("is_int", "stop_ind");//�Ƿ�ͣϢ��־
			map2.put("stpint_dt", "stop_date");//ͣϢ����
			StringBuffer keys2=new StringBuffer("");
			StringBuffer values2=new StringBuffer("");
			for (Map.Entry<String, String> entry2 : map2.entrySet()) { 
				keys2.append(","+entry2.getKey());
				values2.append(","+entry2.getValue());
			}
			System.out.println(keys2.toString().substring(1));
			System.out.println(values2.toString().substring(1));
			//��ʸ��£�������ͼ����
			String ycloans = " SELECT BILL_NO FROM acc_fact a WHERE a.bill_no IN (SELECT b.loan_no FROM lm_loan@YCLOANS b) ";
			PreparedStatement yc = null;
			yc=conn.prepareStatement(ycloans);
			ResultSet rs = yc.executeQuery();
			int count=0;
			Statement pp = conn.createStatement();
			System.out.println(System.currentTimeMillis());
			while(rs.next()){
				String bill_no=rs.getString(1);
//				System.out.println(bill_no);
				count++;
				String low_risk=" UPDATE acc_fact a SET ("+keys2.toString().substring(1)+") = ( SELECT "+values2.toString().substring(1)+" FROM v_lm_loan@YCLOANS b WHERE b.loan_no='"+bill_no+"' )  where  a.bill_no ='"+bill_no+"' ";
//				pp.executeUpdate(low_risk);
				pp.addBatch(low_risk);
				if(count%5000==0){
					pp.executeBatch();
					System.out.println(System.currentTimeMillis());
				}
				
				
			}
			if(count%5000!=0){
				pp.executeBatch();
			}
			System.out.println(System.currentTimeMillis());
			System.out.println(count);
			//�����㲻���ڵ�������Ϊ����
			PreparedStatement up_status_ps=null;
			String status_ps=" UPDATE acc_fact a SET a.account_status='4',a.loan_debt_sts='SETL'  WHERE a.bill_no NOT IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(status_ps);
			up_status_ps=conn.prepareStatement(status_ps);
			up_status_ps.executeUpdate();
			if(up_status_ps!=null){
				up_status_ps.close();
			}
			
			//���½�ݵĻ�����
			PreparedStatement up_due_day_ps=null;
			String up_due_day=" UPDATE acc_fact a SET a.due_day=( select due_day from lm_loan@YCLOANS c where c.loan_no=a.bill_no ) WHERE a.bill_no IN ( SELECT loan_no FROM lm_loan@YCLOANS  )  ";
			System.out.println(up_due_day);
			up_due_day_ps=conn.prepareStatement(up_due_day);
			up_due_day_ps.executeUpdate();
			if(up_due_day_ps!=null){
				up_due_day_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_ps=null;
			String up_prime_rt_knd=" UPDATE acc_fact a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_no = a.bill_no   ) WHERE A.bill_no IN ( SELECT loan_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd);
			up_prime_rt_knd_ps=conn.prepareStatement(up_prime_rt_knd);
			up_prime_rt_knd_ps.executeUpdate();
			if(up_prime_rt_knd_ps!=null){
				up_prime_rt_knd_ps.close();
			}
			
			//���ݺ�������������ͣ��̶����ʣ��������ʣ�
			PreparedStatement up_prime_rt_knd_cont_ps=null;
			String up_prime_rt_knd_cont=" UPDATE ctr_fact_cont a SET a.prime_rt_knd = ( SELECT decode(loan_rate_mode,'RV','1','FX','2') FROM lm_loan_cont@ycloans b WHERE b.loan_cont_no = a.cont_no GROUP BY loan_cont_no,loan_rate_mode  ) WHERE A.cont_no IN ( SELECT loan_cont_no FROM lm_loan_cont@ycloans)  ";
			System.out.println(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps=conn.prepareStatement(up_prime_rt_knd_cont);
			up_prime_rt_knd_cont_ps.executeUpdate();
			if(up_prime_rt_knd_cont_ps!=null){
				up_prime_rt_knd_cont_ps.close();
			}
			
			
			//��������Ϊ��������ʱ�����ʵ�����ʽΪ��
			PreparedStatement up_next_repc_opt_ps=null;
			String up_next_repc_opt=" UPDATE acc_fact a SET next_repc_opt='4' WHERE a.prime_rt_knd='1' AND next_repc_opt IS NULL  ";
			System.out.println(up_next_repc_opt);
			up_next_repc_opt_ps=conn.prepareStatement(up_next_repc_opt);
			up_next_repc_opt_ps.executeUpdate();
			if(up_next_repc_opt_ps!=null){
				up_next_repc_opt_ps.close();
			}
			
			//���ݺ�����»��ʽ
			PreparedStatement up_repayment_mode_ps=null;
			String up_repayment_mode=" UPDATE acc_fact a SET a.repayment_mode = (SELECT loan_paym_mtd FROM lm_loan@ycloans b WHERE a.bill_no=b.loan_no ) WHERE a.bill_no IN (SELECT loan_no FROM lm_loan@ycloans )  ";
			System.out.println(up_repayment_mode);
			up_repayment_mode_ps=conn.prepareStatement(up_repayment_mode);
			up_repayment_mode_ps.executeUpdate();
			if(up_repayment_mode_ps!=null){
				up_repayment_mode_ps.close();
			}
			
			//����������������������ΪǷϢ������Ƿ�������е����ֵ
			PreparedStatement up_overdue_rece_int_ps=null;
			String up_overdue_rece_int=" UPDATE acc_fact a SET overdue_rece_int = CASE WHEN od_days>od_prcp_days THEN od_days ELSE od_prcp_days END  where 1=1  ";
			System.out.println(up_overdue_rece_int);
			up_overdue_rece_int_ps=conn.prepareStatement(up_overdue_rece_int);
			up_overdue_rece_int_ps.executeUpdate();
			if(up_overdue_rece_int_ps!=null){
				up_overdue_rece_int_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���ݺ������ݣ����´���̨�˵Ļ����˺��ֶ�
	public static void test7(Connection conn) {

		try {
			PreparedStatement up_acc_loan_ps=null;
			String up_acc_loan=" UPDATE acc_loan a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_acc_loan_ps=conn.prepareStatement(up_acc_loan);
			up_acc_loan_ps.execute();
			if(up_acc_loan_ps!=null){
				up_acc_loan_ps.close();
			}
			
			PreparedStatement up_acc_fact_ps=null;
			String up_acc_fact=" UPDATE acc_fact a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_acc_fact_ps=conn.prepareStatement(up_acc_fact);
			up_acc_fact_ps.execute();
			if(up_acc_fact_ps!=null){
				up_acc_fact_ps.close();
			}
			
			PreparedStatement up_Acc_Corp_Loan_ps=null;
			String up_Acc_Corp_Loan=" UPDATE Acc_Corp_Loan a SET a.acct_no=( SELECT acct_no FROM lm_acct_info@YCLOANS b where b.loan_no=a.bill_no AND b.acct_typ='PAYM') where a.acct_no is null ";
			up_Acc_Corp_Loan_ps=conn.prepareStatement(up_Acc_Corp_Loan);
			up_Acc_Corp_Loan_ps.execute();
			if(up_Acc_Corp_Loan_ps!=null){
				up_Acc_Corp_Loan_ps.close();
			}
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

	}
}
