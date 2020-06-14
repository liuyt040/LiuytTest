package ncmis.data.D_cont;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import ncmis.data.ConnectPro;

/*
 * �����ͬ��ί�д������
 */
public class CtrLoanCont2CtrLoanCont {

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
			/*test2(conn);
			test3(conn);
			test4(conn);
			testCtrRpy(conn);*/
//			if(first(conn)){
//				test1(conn);
//				test2(conn);
//				test3(conn);
//				test4(conn);
//				testCtrRpy(conn);
//			}else{
//				System.out.println("��ϵͳ���ں�ͬ��ǩ������δ�ſ�ĺ�ͬ����Ҫȫ����ע������������ֲ�������������ų���");
//			}
			
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if (conn != null) {
			conn.close();
		}
	}
	public static boolean first(Connection conn){
		boolean cont_num=true;
		PreparedStatement count_ps=null;
		String count=" SELECT cust_no,CONT_NO,limit_acc_no,limit_ma_no,input_id,cont_state cont_num FROM ctr_loan_cont@CMISTEST2101 a WHERE a.cont_no NOT IN ( SELECT cont_no FROM acc_loan@CMISTEST2101 GROUP BY cont_no ) AND A.limit_ind='2' AND a.cont_state IN ('200','800') ORDER BY cont_state ";
		try {
			count_ps=conn.prepareStatement(count);
			ResultSet rs =count_ps.executeQuery();
			while(rs.next()){
				cont_num=false;
				System.out.println("��ͬ�ţ�"+rs.getString("CONT_NO")+" ����̨�ˣ�"+rs.getString("LIMIT_ACC_NO")+" ����Э�飺"+rs.getString("LIMIT_MA_NO"));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			
		}
		return cont_num;
		
	}

	//��������ͬ����,������ί�кͱ�������һ�ű�
	public static void test1(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//��������
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("cont_no", "cont_no");//��ͬ���
//		map.put("cont_name", "cont_no");//���ĺ�ͬ���
		map.put("gra_avail_amt", "cont_amt");//�������ý��
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//��������
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map.put("fix_int_sprd", "lpr_natu_base_point");//����������
		map.put("other_area_loan", "'010'");//�Ƿ���ش���
		
		//** �Թ��͸��˲�һ������Ҫ���⴦����Ʒ������Ҫӳ��
		/*
		map.put("payment_mode", "''");//֧����ʽ�����ݺ������ݴ���
		map.put("grp_name", "guarantora_grp_name");//����С������
		*/
		map.put("prd_cde", "decode(biz_type,'000015','01029340','010169','01021315','010006','01027580','000027','01026960','000058','01026960','000058','01026960','010273','01026960','010881','01022205','000028','01025910','010374','01026960','010171','01022100','010172','01022100','010173','01022100','000121','01011040','021180','01018946','021060','01018319','000004','01012974','000005','01015330','000017','01012606','000052','01017030','005040','01014350','010005','01011522','010005','01011522','022179','01012731','000006','01017162','000018','01011864','000019','01014244','000020','01013896','000051','01018133','010004','01014172','010007','01014921','010008','01011589','010009','01014730','021061','01017856','022178','01017162','022184','01017863','000091','01023134',biz_type)");//��Ʒ���,��������ѭ����Ʒ�Ͳ�ѭ����Ʒ
		map.put("cont_typ", "decode(cont_type,'2','02','03')");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//�Ƿ�ѭ��
		
		map.put("cus_name", "cust_name");//�ͻ�����
		map.put("cus_id", "cust_no");//�ͻ����
		map.put("cus_typ", "decode(substr(cust_no,1,1),'p','114','c','211')");//�ͻ�
//		map.put("rbp_ind", "");//�Ƿ�����滹
//		map.put("stn_prd_ind", "");//�Ƿ��׼��Ʒ
//		map.put("lr_ind", "");//�Ƿ�ͷ��ղ�Ʒ
		map.put("charge_stamp_duty", "'N'");//�Ƿ���ȡӡ��˰
		map.put("loan_type", "decode(pvp_type,'MBSP','MBSP','ZHSP')");//���˷�ʽ
		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//�������ͣ�������ʽ��
		map.put("limit_ind", "limit_ind");//���ŷ�ʽ
		map.put("lmt_cont_no", "limit_ma_no");//����Э����
		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("appl_ccy", "currency_type");//�������
		map.put("appl_amt", "cont_amt");//�����Ԫ)
		map.put("exc_rt", "exchange_rate");//����
		map.put("tnm_time_typ", "decode(term_time_type,'01','01','02','03','03','06')");//����ʱ������
		map.put("appl_tnr", "loan_term");//��������
		map.put("cont_str_dt", "loan_start_date");//��ͬ��ʼ��
		map.put("cont_end_dt", "loan_end_date");//��ͬ������
		
		map.put("dc_no", "serno");//�������
		
		map.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��׼����
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map.put("ir_exe_typ", "'1'");//����ִ�з�ʽ,ȫ������ͬ����
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
//		map.put("fix_int_sprd", "cont_amt");//�̶�����
		map.put("flt_rt_pct", "cal_floating_rate");//��������
		map.put("rel_ir_y", "reality_ir_y");//ִ������(��)
//		map.put("rel_ir_m", "loan_start_date");//ִ������(��),�޴��ֶ�
		map.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("flt_ovr_rt", "overdue_rate");//���ڼӷ�����
		map.put("flt_dfl_rt", "default_rate");//ΥԼ�ӷ�����
		
//		map.put("fix_ovr_sprd", "loan_start_date");//���ڼӷ�����(û��)
//		map.put("ovr_ir_y1", "exchange_rate");//���ڷ�Ϣ����(û��)
//		map.put("fix_dfl_sprd", "loan_start_date");//ΥԼ�ӷ�����(û��)
//		map.put("dfl_ir_y1", "loan_start_date");//ΥԼ��Ϣ����(û��)
		map.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("dirt_cde", "loan_direction");//����Ͷ�����
		map.put("dirt_name", "direction_name");//����Ͷ������
		map.put("agr_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
		map.put("usg_dsc", "use_dec");//��;����
		
		
		map.put("grn_ind", "'N'");//�Ƿ���ɫ�Ŵ�
//		map.put("gurt_grp_cont_no", "''");//����Э����
		map.put("is_cop", "'N'");//�Ƿ�ռ�ú��������
		
		map.put("BIZ_MDL_ID", "'CC'");//��Ʒģʽ-�ۺ�����
		
		map.put("opr_usr", "cust_mgr");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cust_mgr");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cust_mgr");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cust_mgr");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//Э��״̬
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_loan_cont a  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_loan_cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type not in ('010881','022184','010882') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//�������ڷ�Ϣ����,ΥԼ��Ϣ����
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE ctr_loan_cont a SET a.ovr_ir_y=a.rel_ir_y*1.5,a.dfl_ir_y=a.rel_ir_y*2  ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			//�����Ƿ�ͷ��գ��Թ�ʹ������Ϊ�߷��գ�����Ϊ�ͷ���
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE ctr_loan_cont a SET a.is_low_risk = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'c%' AND b.cont_no=a.cont_no )  ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//�����Ƿ�ͷ��գ����˴�������Ƿ�ͷ����ֶ��ж�
			PreparedStatement up_low_risk=null;
			String low_risk=" UPDATE ctr_loan_cont a SET a.is_low_risk = ( SELECT decode(b.low_risk_flg,'1','Y','N') FROM ctr_loan_cont@CMISTEST2101 b WHERE b.cust_no LIKE 'p%' AND b.cont_no=a.cont_no )";
			up_low_risk=conn.prepareStatement(low_risk);
			up_low_risk.execute();
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE ctr_loan_cont a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde )  ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_loan_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//���¸���֤�����͡�֤������
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE ctr_loan_cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) where a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			//��ѭ����������ʽ�����Ʒ�Ÿ���Ϊ01022512�������ʽ�ѭ�����
			PreparedStatement up_prd_liudong_ps=null;
			String up_prd_liudong=" UPDATE ctr_loan_cont a SET a.prd_cde='01022512',a.prd_name='�����ʽ�ѭ������' WHERE a.prd_cde='01021315' AND a.cont_typ='02' ";
			up_prd_liudong_ps=conn.prepareStatement(up_prd_liudong);
			up_prd_liudong_ps.execute();
			
			
			//���º�ͬ�еĿͻ�����
			PreparedStatement up_cus_typ_ps=null;
			String up_cus_typ=" UPDATE ctr_loan_cont a SET a.cus_typ = ( SELECT cus_type FROM (SELECT b.cus_type,cus_id FROM cus_corp b UNION ALL SELECT c.cus_type,cus_id FROM cus_indiv  c ) d WHERE d.cus_id=a.cus_id)  ";
			up_cus_typ_ps=conn.prepareStatement(up_cus_typ);
			up_cus_typ_ps.execute();
			if(up_cus_typ_ps!=null){
				up_cus_typ_ps.close();
			}
			
			
			//���º�ͬ�е�֧������-����֧��,������κ�һ��̨��Ϊ����֧�������ͬΪ����֧��
			PreparedStatement up_payment_mode_01_ps=null;
			String up_payment_mode_01=" UPDATE ctr_loan_cont a SET a.payment_mode ='01' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='1') ";
			up_payment_mode_01_ps=conn.prepareStatement(up_payment_mode_01);
			up_payment_mode_01_ps.execute();
			if(up_payment_mode_01_ps!=null){
				up_payment_mode_01_ps.close();
			}
			
			//���º�ͬ�е�֧������-����֧����������κ�һ��̨��Ϊ����֧�������ͬ��Ϊ����֧������������֧����ִ�У����Լ���ͬһ�ʺ�ͬ��������֧������������֧��̨�˵Ļ��Ϊ����֧��
			PreparedStatement up_payment_mode_02_ps=null;
			String up_payment_mode_02=" UPDATE ctr_loan_cont a SET a.payment_mode ='02' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='2') ";
			up_payment_mode_02_ps=conn.prepareStatement(up_payment_mode_02);
			up_payment_mode_02_ps.execute();
			if(up_payment_mode_02_ps!=null){
				up_payment_mode_02_ps.close();
			}
			
			//8856820160031 ռ�õ�����̨�˱��Ӧ�ɡ�2016EDTZ88568000004����Ϊ��2016EDTZ88568000002��
			PreparedStatement up_item_id_ps=null;
			String up_item_id=" UPDATE ctr_loan_cont a SET a.lmt_acc_no='2016EDTZ88568000002' WHERE a.cont_no='8856820160031' ";
			up_item_id_ps=conn.prepareStatement(up_item_id);
			up_item_id_ps.execute();
			if(up_item_id_ps!=null){
				up_item_id_ps.close();
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
			if(up_low_risk!=null){
				up_low_risk.close();
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
			if(up_prd_liudong_ps!=null){
				up_prd_liudong_ps.close();
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//����ί�д����ͬ����
	public static void test2(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//��������
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//��������
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map.put("fix_int_sprd", "lpr_natu_base_point");//����������
//		map.put("cont_name", "cont_no");//���ĺ�ͬ���
//		map.put("gra_avail_amt", "cont_amt");//�������ý��
//		map.put("other_area_loan", "'1'");//�Ƿ���ش���		
		map.put("prd_cde", "decode(biz_type,'022184','01017863','010881','01022205')");//��Ʒ���,��������ѭ����Ʒ�Ͳ�ѭ����Ʒ
		map.put("cont_typ", "decode(cont_type,'2','02','03')");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
//		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//�Ƿ�ѭ��
		
		map.put("cus_name", "cust_name");//�ͻ�����
		map.put("cus_id", "cust_no");//�ͻ����
//		map.put("rbp_ind", "");//�Ƿ�����滹
//		map.put("stn_prd_ind", "");//�Ƿ��׼��Ʒ
//		map.put("lr_ind", "");//�Ƿ�ͷ��ղ�Ʒ
        map.put("charge_stamp_duty", "'N'");//�Ƿ���ȡӡ��˰
		
//		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//�������ͣ�������ʽ��
//		map.put("limit_ind", "limit_ind");//���ŷ�ʽ
//		map.put("lmt_cont_no", "limit_ma_no");//����Э����
//		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("appl_ccy", "currency_type");//�������
		map.put("appl_amt", "cont_amt");//�����Ԫ)
//		map.put("exc_rt", "exchange_rate");//����
		map.put("cont_str_dt", "loan_start_date");//��ͬ��ʼ��
		map.put("cont_end_dt", "loan_end_date");//��ͬ������
		map.put("dc_no", "serno");//�������
		map.put("Term_Time_Typ", "decode(term_time_type,'01','01','02','03','03','06')");//����ʱ������
		map.put("appl_tnr", "loan_term");//��������
		
		map.put("ruling_rat_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��׼����
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map.put("ir_exe_typ", "'1'");//����ִ�з�ʽ,ȫ������ͬ����
		map.put("floating_typ", "'01'");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
//		map.put("fix_int_sprd", "cont_amt");//�̶�����
		map.put("floating_pct", "cal_floating_rate");//��������
		map.put("reality_rat_y", "reality_ir_y");//ִ������(��)
//		map.put("rel_ir_m", "loan_start_date");//ִ������(��),�޴��ֶ�
		map.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("overdue_rat", "overdue_rate");//���ڼӷ�����
		map.put("overdue_rat_m", "overdue_ir*12");//���ڷ�Ϣ����(�꣩
		map.put("default_rat", "default_rate");//ΥԼ�ӷ�����
		map.put("default_rat_m", "default_ir*12");//ΥԼ��Ϣ����(��)
		
//		map.put("fix_ovr_sprd", "loan_start_date");//���ڼӷ�����(û��)
//		map.put("ovr_ir_y1", "exchange_rate");//���ڷ�Ϣ����(û��)
//		map.put("fix_dfl_sprd", "loan_start_date");//ΥԼ�ӷ�����(û��)
//		map.put("dfl_ir_y1", "loan_start_date");//ΥԼ��Ϣ����(û��)
		map.put("rat_adjust_mode", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("loan_direction", "loan_direction");//����Ͷ�����
		map.put("direction_name", "direction_name");//����Ͷ������
		map.put("agriculture_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map.put("agriculture_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
		map.put("use_dec", "use_dec");//��;����
		map.put("principal_loan_typ", "decode(loan_form,'1','1','3','2','1')");
		
//		map.put("gurt_grp_cont_no", "''");//����Э����
//		map.put("is_cop", "'N'");//�Ƿ�ռ�ú��������
		
//		map.put("BIZ_MDL_ID", "'CC'");//��Ʒģʽ-�ۺ�����
		
		map.put("opr_usr", "cust_mgr");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cust_mgr");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cust_mgr");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cust_mgr");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//Э��״̬
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_corp_loan_entr a   ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO ctr_corp_loan_entr ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type  in ('010881','022184') )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			//�������ڷ�Ϣ����,ΥԼ��Ϣ����
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE ctr_corp_loan_entr a SET a.overdue_rat_m=a.reality_rat_y*1.5,a.default_rat_m=a.reality_rat_y*2 ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE ctr_corp_loan_entr a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde ) ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
			up_cert_typ_ps=conn.prepareStatement(up_cert_typ);
			up_cert_typ_ps.execute();
			
			//���¸���֤�����͡�֤������
			PreparedStatement up_cert_typ_01_ps=null;
			String up_cert_typ_01=" UPDATE ctr_corp_loan_entr a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_indiv b WHERE a.cus_id=b.cus_id ) where a.cus_id LIKE 'p%' ";
			up_cert_typ_01_ps=conn.prepareStatement(up_cert_typ_01);
			up_cert_typ_01_ps.execute();
			
			if(insert_cus_corp_ps!=null){
				insert_cus_corp_ps.close();
			}
			/*
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}
			if(up_low_risk!=null){
				up_low_risk.close();
			}*/
			if(up_prd_name_ps!=null){
				up_prd_name_ps.close();
			}
			if(up_cert_typ_ps!=null){
				up_cert_typ_ps.close();
			}
			if(up_cert_typ_01_ps!=null){
				up_cert_typ_01_ps.close();
			}
			
			
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	/*
	 * ί�д���ӱ�
	 */
	public static void test4(Connection conn) {
		Map<String,String> map3 = new HashMap<String,String>();
		map3.put("principal_typ", "decode(principal_type,'04','5','03','3','01','241','1')");
//		map3.put("principal_loan_typ", "decode(loan_form,'1','1','3','2','1')");
		map3.put("principal_cus_id", "principal_cus_id");
		map3.put("principal_name", "principal_name");
		map3.put("principa_cert_typ", "decode(principa_cert_type,'10','10100','20','20100','21','21400','22','20200','23','20400','24','29902','25','21300','26','21301','27','21302','29','22600','29900')");
		map3.put("principa_cert_cde", "principa_cert_code");
		map3.put("comm_charge_rate", "comm_charge_rate");
		map3.put("comm_charge_amt", "comm_charge_amt");
		map3.put("principal_bal_acct", "principal_bal_acct");
		map3.put("entrust_cond", "entrust_cond");
		
		StringBuffer keys3=new StringBuffer("");
		StringBuffer values3=new StringBuffer("");
		for (Map.Entry<String, String> entry3 : map3.entrySet()) { 
			keys3.append(","+entry3.getKey());
			values3.append(","+entry3.getValue());
		}
		
		PreparedStatement principal_ps=null;
		String principal=" UPDATE ctr_corp_loan_entr a SET ("+keys3.toString().substring(1)+") = ( SELECT "+values3.toString().substring(1)+" FROM Ctr_Com_Loan_Entr@CMISTEST2101 b WHERE b.cont_no=a.cont_no ) where a.cont_no in ( select cont_no from ctr_loan_cont@CMISTEST2101) ";
		System.out.println(principal);
		try {
			principal_ps=conn.prepareStatement(principal);
			principal_ps.execute();
			if(principal_ps!=null){
				principal_ps.close();
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	//���뱣���ͬ����
	public static void test3(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		//��ϵͳȱ��ע�����������reg_state_code����������������reg_area_name
		map=new HashMap<String, String>();
		map.put("iqp_cde", "serno");//��������
		map.put("serno", "serno");//ҵ����ˮ��
		map.put("cont_no", "cont_no");//��ͬ���
		map.put("base_rt_knd", "decode(rate_type,'2','30',10)");//��������
		map.put("base_lpr_rt_knd", "decode(lpr_rate_no,'LP1','LPR1','LP5','LPR5')");//LPR��������
		map.put("fix_int_sprd", "lpr_natu_base_point");//����������
		map.put("gra_avail_amt", "cont_amt");//�������ý��
//		map.put("other_area_loan", "'1'");//�Ƿ���ش���
		
		//** �Թ��͸��˲�һ������Ҫ���⴦����Ʒ������Ҫӳ��
		/*
		map.put("payment_mode", "''");//֧����ʽ�����ݺ������ݴ���
		map.put("grp_name", "guarantora_grp_name");//����С������
		*/
		map.put("prd_cde", "'01029525'");//��Ʒ���
		map.put("cont_typ", "decode(cont_type,'2','02','01')");//��ͬ���ͣ�ѭ����ͬ��һ���ͬ
		map.put("cycle_ind", "decode(cont_type,'2','Y','N')");//�Ƿ�ѭ��
		
		map.put("cus_name", "cust_name");//�ͻ�����
		map.put("cus_id", "cust_no");//�ͻ����
//		map.put("rbp_ind", "");//�Ƿ�����滹
//		map.put("stn_prd_ind", "");//�Ƿ��׼��Ʒ
//		map.put("lr_ind", "");//�Ƿ�ͷ��ղ�Ʒ
        map.put("charge_stamp_duty", "'N'");//�Ƿ���ȡӡ��˰
		
		map.put("loan_frm", "decode(loan_form,'1','1','3','2','5','3')");//�������ͣ�������ʽ��
		map.put("limit_ind", "limit_ind");//���ŷ�ʽ
		map.put("lmt_cont_no", "limit_ma_no");//����Э����
		map.put("lmt_acc_no", "limit_acc_no");//���̨�ʱ��
		map.put("appl_ccy", "currency_type");//�������
		map.put("appl_amt", "cont_amt");//�����Ԫ)
		map.put("exc_rt", "exchange_rate");//����
		map.put("cont_str_dt", "loan_start_date");//��ͬ��ʼ��
		map.put("cont_end_dt", "loan_end_date");//��ͬ������
		map.put("tnm_time_typ", "decode(term_time_type,'01','01','02','03','03','06')");//����ʱ������
		map.put("appl_tnr", "loan_term");//��������
		
		map.put("dc_no", "serno");//�������
		
		map.put("base_ir_y", "decode(rate_type,'2',lpr_rate,ruling_ir)");//��׼����
		map.put("prime_rt_knd", "decode(rate_exe_model,'11','2','12','1')");//��������
		map.put("disc_ind", "decode(discont_ind,'1','Y','2','N')");//�Ƿ���Ϣ
		map.put("ir_exe_typ", "'1'");//����ִ�з�ʽ,ȫ������ͬ����
		map.put("ir_flt_typ", "decode(rate_type,'2','02','01')");//���ʸ�������,ȫ������������
		map.put("fix_int_rt", "reality_ir_y");//�̶�����,ִ������
//		map.put("fix_int_sprd", "cont_amt");//�̶�����
		map.put("flt_rt_pct", "cal_floating_rate");//��������
		map.put("rel_ir_y", "reality_ir_y");//ִ������(��)
//		map.put("rel_ir_m", "loan_start_date");//ִ������(��),�޴��ֶ�
		map.put("od_fine_flt_typ", "'01'");//��Ϣ���ʸ�������,ȫ����ִ������
		map.put("od_cmp_mde", "'1'");//��Ϣ���㷽ʽ��ȫ������������
		map.put("flt_ovr_rt", "overdue_rate");//���ڼӷ�����
		map.put("ovr_ir_m", "overdue_ir*12");//���ڷ�Ϣ����(�꣩ovr_ir_m
		map.put("flt_dfl_rt", "default_rate");//ΥԼ�ӷ�����
		map.put("dfl_ir_y", "default_ir*12");//ΥԼ��Ϣ����(��)
		
//		map.put("fix_ovr_sprd", "loan_start_date");//���ڼӷ�����(û��)
//		map.put("ovr_ir_y1", "exchange_rate");//���ڷ�Ϣ����(û��)
//		map.put("fix_dfl_sprd", "loan_start_date");//ΥԼ�ӷ�����(û��)
//		map.put("dfl_ir_y1", "loan_start_date");//ΥԼ��Ϣ����(û��)
		map.put("next_repc_opt", "decode(rate_exe_model,'12','4','11','')");//���ʵ�����ʽ,��Ϊ�������ʣ�ֻ����һ��һ��
		
		map.put("gurt_typ", "decode(assure_means_main,'00','40',assure_means_main)");//��������ʽ
		map.put("oth_gurt_typ1", "decode(assure_means2,'00','40',assure_means2)");//������ʽ1
		map.put("oth_gurt_typ2", "decode(assure_means3,'00','40',assure_means3)");//������ʽ2
		map.put("dirt_cde", "loan_direction");//����Ͷ�����
		map.put("dirt_name", "direction_name");//����Ͷ������
		map.put("agr_typ", "agriculture_type");//��ũ����,��ֵһ�£�����ת��
		map.put("agr_use", "decode(agriculture_use,'1000','1101','1110','2210','1120','2220','1130','2230','1141','2241','1142','2241','1150','2250','1160','2260','1200','2261')");//��ũ��;
		map.put("use_dec", "use_dec");//��;����
		
		
//		map.put("grn_ind", "loan_direction");//�Ƿ���ɫ�Ŵ�
//		map.put("gurt_grp_cont_no", "''");//����Э����
		map.put("is_cop", "'N'");//�Ƿ�ռ�ú��������
		
//		map.put("BIZ_MDL_ID", "'CC'");//��Ʒģʽ-�ۺ�����
		
		map.put("opr_usr", "cust_mgr");//������
		map.put("opr_bch", "input_br_id");//�������
		map.put("opr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("coopr_usr", "cust_mgr");//Э����
		map.put("coopr_bch", "input_br_id");//Э�����
		map.put("coopr_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("crt_usr", "cust_mgr");//�Ǽ���
		map.put("crt_bch", "input_br_id");//�Ǽǻ���
		map.put("crt_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		map.put("main_usr", "cust_mgr");//������
		map.put("main_bch", "input_br_id");//���ܻ���
		map.put("iqp_dt", "sign_date");//����ʱ�䣬�Ȱ���ͬǩ��ʱ��ˢһ�飬��Ϊ�յ��ٰ�����ʱ��ˢһ��
		
		map.put("cont_sts", "decode(cont_state,'100','02','200','10','300','42','999','90')");//Э��״̬
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

		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from Ctr_Fact_Cont a  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement insert_cus_corp_ps=null;
			String insert_cus_corp=" INSERT INTO Ctr_Fact_Cont ("+keys.toString().substring(1)+") (SELECT "+values.toString().substring(1)+" FROM ctr_loan_cont@CMISTEST2101 b where b.biz_type  = '010882' )";
			System.out.println(insert_cus_corp);
			insert_cus_corp_ps=conn.prepareStatement(insert_cus_corp);
			insert_cus_corp_ps.execute();
			
			//�������ڷ�Ϣ����,ΥԼ��Ϣ����
			PreparedStatement ovr_ir_y_ps=null;
			String ovr_ir_y=" UPDATE Ctr_Fact_Cont a SET a.ovr_ir_m=rel_ir_y*1.5,dfl_ir_y=rel_ir_y*2 ";
			ovr_ir_y_ps=conn.prepareStatement(ovr_ir_y);
			ovr_ir_y_ps.execute();
			if(ovr_ir_y_ps!=null){
				ovr_ir_y_ps.close();
			}
			
			//���º�ͬ�е�֧������-����֧��,������κ�һ��̨��Ϊ����֧�������ͬΪ����֧��
			PreparedStatement up_payment_mode_01_ps=null;
			String up_payment_mode_01=" UPDATE Ctr_Fact_Cont a SET a.payment_mode ='01' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='1') ";
			up_payment_mode_01_ps=conn.prepareStatement(up_payment_mode_01);
			up_payment_mode_01_ps.execute();
			if(up_payment_mode_01_ps!=null){
				up_payment_mode_01_ps.close();
			}
			
			//���º�ͬ�е�֧������-����֧����������κ�һ��̨��Ϊ����֧�������ͬ��Ϊ����֧������������֧����ִ�У����Լ���ͬһ�ʺ�ͬ��������֧������������֧��̨�˵Ļ��Ϊ����֧��
			PreparedStatement up_payment_mode_02_ps=null;
			String up_payment_mode_02=" UPDATE Ctr_Fact_Cont a SET a.payment_mode ='02' WHERE a.cont_no IN (SELECT cont_no FROM  acc_loan@CMISTEST2101 WHERE payment_type='2') ";
			up_payment_mode_02_ps=conn.prepareStatement(up_payment_mode_02);
			up_payment_mode_02_ps.execute();
			if(up_payment_mode_02_ps!=null){
				up_payment_mode_02_ps.close();
			}
			
			//�����Ƿ�ͷ��գ��Թ�ʹ������Ϊ�߷��գ�����Ϊ�ͷ���
			PreparedStatement up_main_br_id=null;
			String main_br_id=" UPDATE Ctr_Fact_Cont a SET a.lr_ind = ( SELECT decode(b.limit_ind,'2','N','Y') FROM ctr_loan_cont@CMISTEST2101 b WHERE  b.cont_no=a.cont_no ) ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();
			
			//���²�Ʒ����
			PreparedStatement up_prd_name_ps=null;
			String up_prd_name=" UPDATE Ctr_Fact_Cont a SET (a.prd_name,cl_typ,cl_typ_sub) = ( SELECT b.prd_name,b.cl_typ,b.cl_typ_sub FROM prd_base_info b WHERE b.prd_cde=a.prd_cde )  ";
			up_prd_name_ps=conn.prepareStatement(up_prd_name);
			up_prd_name_ps.execute();
			
			//���¶Թ�֤�����͡�֤������
			PreparedStatement up_cert_typ_ps=null;
			String up_cert_typ=" UPDATE Ctr_Fact_Cont a SET (a.cert_typ,a.cert_cde) = (SELECT cert_type,b.cert_code FROM cus_corp b WHERE a.cus_id=b.cus_id ) where  a.cus_id LIKE 'c%' ";
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
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	//���뻹�ʽ 
	public static void testCtrRpy(Connection conn) {
		//key:�±��ֶ�   
		//value:�ϱ��ֶ�
		try {
			PreparedStatement de_ps_cus_corp=null;
			String de_cus_corp=" delete from ctr_rpy  ";
			de_ps_cus_corp=conn.prepareStatement(de_cus_corp);
			de_ps_cus_corp.execute();
			
			PreparedStatement up_main_br_id=null;
			String main_br_id=" INSERT INTO CTR_RPY (PK_ID, CONT_NO, MTD_CDE, RPY_SRC_DSC, MTD_FREQ_UNIT,mtd_freq,repay_day,repay_date) SELECT 'pk' || CONT_NO,CONT_NO,DECODE(REPAYMENT_MODE,'101','CL17','102','CL15','201','CL21','202','CL18','205','CL31'),REPAYMENT_SRC_DEC,DECODE(INTEREST_ACC_MODE,'03','03','04','04','06','03',INTEREST_ACC_MODE),'1',decode(repayment_mode,'201','20','202','20','21'),'10' FROM CTR_LOAN_CONT@CMISTEST2101 ";
			up_main_br_id=conn.prepareStatement(main_br_id);
			up_main_br_id.execute();

			
			PreparedStatement up_mtd_freq_unit=null;
			String mtd_freq_unit=" UPDATE ctr_rpy a SET mtd_freq_unit = '03',a.mtd_freq='1' WHERE a.mtd_freq_unit='01' ";
			up_mtd_freq_unit=conn.prepareStatement(mtd_freq_unit);
			up_mtd_freq_unit.execute();
			if(up_mtd_freq_unit!=null){
				up_mtd_freq_unit.close();
			}
			
			PreparedStatement up_mtd_freq_unit2=null;
			String mtd_freq_unit2=" UPDATE ctr_rpy a SET mtd_freq_unit = '04',a.mtd_freq='2' WHERE a.mtd_freq_unit='08'  ";//������
			up_mtd_freq_unit2=conn.prepareStatement(mtd_freq_unit2);
			up_mtd_freq_unit2.execute();
			if(up_mtd_freq_unit2!=null){
				up_mtd_freq_unit2.close();
			}
			
			PreparedStatement up_mtd_freq_unit4=null;
			String mtd_freq_unit4=" UPDATE ctr_rpy a SET mtd_freq_unit = '01',a.mtd_freq='1' WHERE a.mtd_freq_unit='02'  ";//����
			up_mtd_freq_unit4=conn.prepareStatement(mtd_freq_unit4);
			up_mtd_freq_unit4.execute();
			if(up_mtd_freq_unit4!=null){
				up_mtd_freq_unit4.close();
			}
			
			PreparedStatement up_mtd_freq_unit3=null;
			String mtd_freq_unit3=" UPDATE ctr_rpy a SET mtd_freq_unit = '02',a.mtd_freq='1' WHERE a.mtd_freq_unit='07'  ";//����
			up_mtd_freq_unit3=conn.prepareStatement(mtd_freq_unit3);
			up_mtd_freq_unit3.execute();
			if(up_mtd_freq_unit3!=null){
				up_mtd_freq_unit3.close();
			}
			
			
			
			if(de_ps_cus_corp!=null){
				de_ps_cus_corp.close();
			}
			if(up_main_br_id!=null){
				up_main_br_id.close();
			}

		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	}
	
	

}

