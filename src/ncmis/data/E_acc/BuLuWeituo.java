package ncmis.data.E_acc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

public class BuLuWeituo {

	
	public static void main(String[] arg){
		
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
			Connection conn = null;
			conn = DriverManager.getConnection(ConnectPro.url,ConnectPro.username ,ConnectPro.passwd);
			buluCont(conn);
			buluAcc(conn);
			buluWeiDai(conn);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	public static void buluCont(Connection conn) throws SQLException{
		
		
		Map<String,String>map=new HashMap<String, String>();
//		map.put("serno", "htwd8808820150001");//��������
//		map.put("cont_no", "htwd8808820150001");//��ͬ���
//		map.put("appl_amt", "20000000");//��ͬ���
//		map.put("biz_type", "'010881'");//��Ʒ���
//		map.put("prd_name", "'�Թ�ί�д���'");//��Ʒ���
//		map.put("cont_type", "'3'");//��ͬ������߷�����
//		
//		
////		map.put("cust_name", "'��̨��һ������������޹�˾ '");//�ͻ�����
//		map.put("cust_no", "'c011428981'");//�ͻ����
//		map.put("pvp_type", "'CMIS'");//���˷�ʽ
//		map.put("loan_form", "'1'");//�������ͣ�������ʽ��
//		map.put("limit_ind", "'1'");//���ŷ�ʽ
//		map.put("limit_ma_no", "''");//����Э����
//		map.put("limit_acc_no", "''");//���̨�ʱ��
//		map.put("currency_type", "'CNY'");//�������
//		map.put("exchange_rate", "1");//����
//		map.put("term_time_type", "'02'");//����ʱ������
//		map.put("loan_term", "6");//��������
//		map.put("loan_start_date", "'2015-04-28'");//��ͬ��ʼ��
//		map.put("loan_end_date", "'2015-10-28'");//��ͬ������
//		
//		
//		map.put("prime_rt_knd", "'2'");//��������
//		map.put("ruling_rat_y", "0.0851265");//��׼����(��)
//		map.put("rate_exe_model", "'11'");//��������**
//		map.put("discont_ind", "'2'");//�Ƿ���Ϣ
//		map.put("reality_ir_y", "0.144");//ִ��������
//		map.put("cal_floating_rate", "1.6916");//��������
//		map.put("flt_ovr_rt", "0.5");//���ڼӷ�����
//		map.put("overdue_ir", "0.018");//���ڷ�Ϣ����(�£�
//		map.put("flt_dfl_rt", "1");//ΥԼ�ӷ�����
//		map.put("default_ir", "0.024");//ΥԼ��Ϣ����(��)
//
//		map.put("rate_exe_model", "'11'");//���ʵ�����ʽ
//		
//		map.put("assure_means_main", "'00'");//��������ʽ
//		map.put("assure_means2", "''");//������ʽ1
//		map.put("assure_means3", "''");//������ʽ2
//		map.put("loan_direction", "''");//����Ͷ�����
//		map.put("direction_name", "''");//����Ͷ������
//		map.put("agriculture_type", "''");//��ũ����,��ֵһ�£�����ת��
//		map.put("agriculture_use", "''");//��ũ��;
//		map.put("use_dec", "''");//��;����
//		
//		map.put("repayment_mode", "'102'");//���ʽ
//		map.put("interest_acc_mode", "'03'");//���»�Ϣ
//		
//		map.put("cust_mgr", "'1359'");//������
//		map.put("input_br_id", "'88088'");//�������
//		map.put("sign_date", "'20150428'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
//
//		map.put("input_id", "'1359'");//¼����
//		map.put("main_br_id", "'88088'");//���ܻ���
//		
//		map.put("cont_state", "'200'");//Э��״̬
//		
		
		//**************
		//*************
		//*************
		//*************
		//*************
		//*************
		//*************
		map=new HashMap<String, String>();
		map.put("iqp_cde", "'htwd8808820150001'");//��������
		map.put("serno", "'htwd8808820150001'");//ҵ����ˮ��
		map.put("cont_no", "'htwd8808820150001'");//��ͬ���
		map.put("prd_cde", "'01022205'");//��Ʒ���,��������ѭ����Ʒ�Ͳ�ѭ����Ʒ
		map.put("cont_typ", "'03'");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
		
		map.put("cus_name", "'��̨��ҵ´����������޹�˾'");//�ͻ�����
		map.put("cus_id", "'c011428981'");//�ͻ����
        map.put("charge_stamp_duty", "'N'");//�Ƿ���ȡӡ��˰
		
		map.put("appl_ccy", "'CNY'");//�������
		map.put("appl_amt", "20000000");//�����Ԫ)
		map.put("cont_str_dt", "'2015-04-28'");//��ͬ��ʼ��
		map.put("cont_end_dt", "'2015-10-28'");//��ͬ������
		map.put("dc_no", "'htwd8808820150001'");//�������
		map.put("Term_Time_Typ", "'03'");//����ʱ������
		map.put("appl_tnr", "6");//��������
		
		map.put("ruling_rat_y", "0.0851265");//��׼����
		map.put("prime_rt_knd", "'2'");//��������
		map.put("disc_ind", "'N'");//�Ƿ���Ϣ
		map.put("ir_exe_typ", "'1'");//����ִ�з�ʽ,ȫ������ͬ����
		map.put("floating_typ", "'01'");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "0.144");//�̶�����,ִ������
		map.put("reality_rat_y", "0.144");//ִ������(��)
		map.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("overdue_rat", "0.5");//���ڼӷ�����
		map.put("overdue_rat_m", "0.216");//���ڷ�Ϣ����(�꣩
		map.put("default_rat", "1");//ΥԼ�ӷ�����
		map.put("default_rat_m", "0.288");//ΥԼ��Ϣ����(��)
		
		map.put("rat_adjust_mode", "'4'");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		
		map.put("gurt_typ", "'40'");//��������ʽ
//		map.put("loan_direction", "''");//����Ͷ�����
//		map.put("direction_name", "direction_name");//����Ͷ������
//		map.put("agriculture_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
//		map.put("agriculture_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
//		map.put("use_dec", "use_dec");//��;����
		map.put("principal_loan_typ", "'1'");
		
		
		map.put("opr_usr", "'1359'");//������
		map.put("opr_bch", "'88088'");//�������
		map.put("opr_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "'1359'");//Э����
		map.put("coopr_bch", "'88088'");//Э�����
		map.put("coopr_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "'1359'");//�Ǽ���
		map.put("crt_bch", "'88088'");//�Ǽǻ���
		map.put("crt_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "'1359'");//������
		map.put("main_bch", "'88088'");//���ܻ���
		map.put("iqp_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("cont_sts", "'10'");//Э��״̬
		map.put("wf_appr_sts", "'997'");//����״̬��ȫ��Ϊͨ��
		
		map.put("instu_cde", "'0000'");//���˱���
		StringBuffer keys=new StringBuffer("");
		StringBuffer values=new StringBuffer("");
		for (Map.Entry<String, String> entry : map.entrySet()) { 
			keys.append(","+entry.getKey());
			values.append(","+entry.getValue());
		}
		System.out.println(keys.toString().substring(1));
		System.out.println(values.toString().substring(1));
		
		PreparedStatement de_ps_cus_corp=null;
		String de_cus_corp=" delete from ctr_corp_loan_entr where cont_no='htwd8808820150001' ";
		de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
		de_ps_cus_corp.execute();
		
		PreparedStatement insert_cus_corp_ps=null;
		String insert_cus_corp=" INSERT INTO ctr_corp_loan_entr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM dual)";
		System.out.println(insert_cus_corp);
		insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
		insert_cus_corp_ps.execute();
		
		//���¸���֤�����͡�֤������
		PreparedStatement up_cert_typ_01_ps=null;
		String up_cert_typ_01=" UPDATE ctr_corp_loan_entr a SET cus_name = (SELECT cus_name FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cont_no='htwd8808820150001'";
		up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
		up_cert_typ_01_ps.execute();
		
		//���¶Թ�֤�����͡�֤������
		PreparedStatement up_cert_typ_ps=null;
		String up_cert_typ=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cont_no='htwd8808820150001' ";
		up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
		up_cert_typ_ps.execute();
		
		PreparedStatement up_prd_name_ps=null;
		String up_prd_name=" UPDATE ctr_corp_loan_entr a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) where cont_no='htwd8808820150001'";
		up_prd_name_ps=conn.prepareStatement(up_prd_name);
		up_prd_name_ps.execute();
	}
	
	public static void buluAcc(Connection conn) throws SQLException{
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		Map<String,String> map=new HashMap<String, String>();
		map.put("account_status", "'2'");//̨��״̬,���ݺ���״̬���޸�����������״̬
		map.put("cont_no", "'htwd8808820150001'");//��ͬ���
		map.put("bill_no", "'htwd8808820150001-1'");//��ݱ��
		map.put("cus_name", "'��̨��ҵ´����������޹�˾'");//�ͻ�����
		map.put("cus_id", "'c011428981'");//�ͻ����
		
		map.put("base_rt_knd", "'10'");//��׼��������
		
		map.put("prd_cde", "'01022205'");
		map.put("cur_type", "'CNY'");//�������
		map.put("loan_amount", "20000000");//�����Ԫ)
		map.put("loan_start_date", "'2015-04-28'");//�������
		map.put("loan_end_date", "'2015-10-28'");//������ֹ��
		
		map.put("ruling_ir", "0.0851265");//��׼����
		map.put("ir_flt_typ", "'01'");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "0.144");//�̶�����,ִ������
		map.put("floating_pct", "1.6916");//��������
		map.put("rel_ir_y", "0.144");//ִ������(��)
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("ovr_ir_y", "0.216");//���ڷ�Ϣ����(�꣩
		map.put("flt_ovr_rt", "0.5");//���ڼӷ�����
		map.put("flt_dfl_rt", "1");//ΥԼ�ӷ�����
		map.put("dfl_ir_y", "0.288");//ΥԼ��Ϣ����(��)
		map.put("assure_means_main", "'40'");//��������ʽ
		map.put("repayment_mode", "'CL15'");//���ʽ
		map.put("loan_balance", "12000000");//�������(Ԫ)
		map.put("revolving_times", "''");//���»��ɴ���
		map.put("extension_times", "''");//չ�ڴ���
		map.put("cla", "''");//�弶����
		
		map.put("prime_rt_knd", "'2'");//��������
		map.put("disc_ind", "'N'");//�Ƿ���Ϣ
		map.put("next_repc_opt", "'4'");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		map.put("dirt_cde", "''");//����Ͷ�����
		map.put("dirt_name", "''");//����Ͷ������
		map.put("agr_typ", "''");//��ũ����,��ֵһ�£�����ת��
		map.put("serno", "'htwd8808820150001'");//ҵ����ˮ��
		map.put("cont_typ", "'03'");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
		map.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map.put("usg_dsc", "''");//��;����
		
		map.put("opr_usr", "'1359'");//������
		map.put("opr_bch", "'88088'");//�������
		map.put("opr_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "'1359'");//Э����
		map.put("coopr_bch", "'88088'");//Э�����
		map.put("coopr_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "'1359'");//�Ǽ���
		map.put("crt_bch", "'88088'");//�Ǽǻ���
		map.put("crt_dt", "'2015-04-28'");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("cus_manager", "'1359'");//������
		map.put("main_br_id", "'88088'");//���ܻ���

		
		map.put("instu_cde", "'0000'");//���˱���
		
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
			String de_cus_corp=" delete from Acc_Corp_Loan a where a.bill_no='htwd8808820150001-1' ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Acc_Corp_Loan ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM dual)";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			
			
			//���¸���֤�����͡�֤������
			PreparedStatement up_cert_typ_02_ps=null;
			String up_cert_typ_02=" UPDATE Acc_Corp_Loan a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) WHERE a.cont_no='htwd8808820150001' ";
			up_cert_typ_02_ps=conn.prepareStatement(up_cert_typ_02);
			up_cert_typ_02_ps.execute();
			
			
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Acc_Corp_Loan a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) where cont_no='htwd8808820150001' ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
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
	
	public static void buluWeiDai(Connection conn) throws SQLException{

		PreparedStatement up1_ps=null;
		String up1=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p008669724','���纣','10100','130504196605171515',0.002,20000,'6212385001000053767','��' FROM dual) WHERE a.cont_no='htwd8808820140001' ";
		up1_ps=conn.prepareStatement(up1);
		up1_ps.execute();
		if(up1_ps!=null){
			up1_ps.close();
		}
		
		PreparedStatement up2_ps=null;
		String up2=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p000101356','���÷','10100','130504196312191520',0.002,20000,'6212385001000110344','��' FROM dual) WHERE a.cont_no='htwd8808820140007' ";
		up2_ps=conn.prepareStatement(up2);
		up2_ps.execute();
		if(up2_ps!=null){
			up2_ps.close();
		}
		
		PreparedStatement up3_ps=null;
		String up3=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','5','p008669724','���纣','10100','130504196605171515',0.002,40000,'6212385001000053767','��' FROM dual) WHERE a.cont_no='htwd8808820150001' ";
		up3_ps=conn.prepareStatement(up3);
		up3_ps.execute();
		if(up3_ps!=null){
			up3_ps.close();
		}
		
		PreparedStatement up4_ps=null;
		String up4=" UPDATE ctr_corp_loan_entr a SET (a.principal_loan_typ,a.principal_typ,a.principal_cus_id,principal_name,principa_cert_typ,principa_cert_cde,comm_charge_rate,comm_charge_amt,principal_bal_acct,entrust_cond) = (SELECT '1','1','c012325228','��̨��ũũҵ�����������޹�˾','20100','05546998-1',0.002,40000,'8801812002000002813','��' FROM dual) WHERE a.cont_no='Ӫҵ��ί��2014001��' ";
		up4_ps=conn.prepareStatement(up4);
		up4_ps.execute();
		if(up4_ps!=null){
			up4_ps.close();
		}
	}
	
}
