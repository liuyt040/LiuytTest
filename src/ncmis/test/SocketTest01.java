package ncmis.test;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;

public class SocketTest01{

    public static void main(String... args) throws IOException{
    	System.out.println((int)(Math.random()*10000));
    	
    	SocketTest01 s = new SocketTest01();
    	ThreadTest te1 = s.new ThreadTest();
    	te1.start();
    	/*
    	ThreadTest te2 = s.new ThreadTest();
    	te2.start();
    	ThreadTest te3 = s.new ThreadTest();
    	te3.start();
    	ThreadTest te4 = s.new ThreadTest();
    	te4.start();
    	ThreadTest te5 = s.new ThreadTest();
    	te5.start();
    	ThreadTest te6 = s.new ThreadTest();
    	te6.start();
    	ThreadTest te7 = s.new ThreadTest();
    	te7.start();
    	ThreadTest te8 = s.new ThreadTest();
    	te8.start();
    	ThreadTest te9 = s.new ThreadTest();
    	te9.start();
    	ThreadTest te10 = s.new ThreadTest();
    	te10.start();
    	
    	ThreadTest te11 = s.new ThreadTest();
    	te11.start();
    	ThreadTest te12 = s.new ThreadTest();
    	te12.start();
    	ThreadTest te13 = s.new ThreadTest();
    	te13.start();
    	ThreadTest te14 = s.new ThreadTest();
    	te14.start();
    	ThreadTest te15 = s.new ThreadTest();
    	te15.start();
    	ThreadTest te16 = s.new ThreadTest();
    	te16.start();
    	ThreadTest te17 = s.new ThreadTest();
    	te17.start();
    	ThreadTest te18 = s.new ThreadTest();
    	te18.start();
    	ThreadTest te19 = s.new ThreadTest();
    	te19.start();
    	ThreadTest te20 = s.new ThreadTest();
    	te20.start();
    	
    	ThreadTest te21 = s.new ThreadTest();
    	te21.start();
    	ThreadTest te22 = s.new ThreadTest();
    	te22.start();
    	ThreadTest te23 = s.new ThreadTest();
    	te23.start();
    	ThreadTest te24 = s.new ThreadTest();
    	te24.start();
    	ThreadTest te25 = s.new ThreadTest();
    	te25.start();
    	ThreadTest te26 = s.new ThreadTest();
    	te26.start();
    	ThreadTest te27 = s.new ThreadTest();
    	te27.start();
    	ThreadTest te28 = s.new ThreadTest();
    	te28.start();
    	ThreadTest te29 = s.new ThreadTest();
    	te29.start();
    	ThreadTest te30 = s.new ThreadTest();
    	te30.start();
    	
    	ThreadTest te31 = s.new ThreadTest();
    	te31.start();
    	ThreadTest te32 = s.new ThreadTest();
    	te32.start();
    	ThreadTest te33 = s.new ThreadTest();
    	te33.start();
    	ThreadTest te34 = s.new ThreadTest();
    	te34.start();
    	ThreadTest te35 = s.new ThreadTest();
    	te35.start();
    	ThreadTest te36 = s.new ThreadTest();
    	te36.start();
    	ThreadTest te37 = s.new ThreadTest();
    	te37.start();
    	ThreadTest te38 = s.new ThreadTest();
    	te38.start();
    	ThreadTest te39 = s.new ThreadTest();
    	te39.start();
    	ThreadTest te40 = s.new ThreadTest();
    	te40.start();
    	
    	ThreadTest te41 = s.new ThreadTest();
    	te41.start();
    	ThreadTest te42 = s.new ThreadTest();
    	te42.start();
    	ThreadTest te43 = s.new ThreadTest();
    	te43.start();
    	ThreadTest te44 = s.new ThreadTest();
    	te44.start();
    	ThreadTest te45 = s.new ThreadTest();
    	te45.start();
    	ThreadTest te46 = s.new ThreadTest();
    	te46.start();
    	ThreadTest te47 = s.new ThreadTest();
    	te47.start();
    	ThreadTest te48 = s.new ThreadTest();
    	te48.start();
    	ThreadTest te49 = s.new ThreadTest();
    	te49.start();
    	ThreadTest te50 = s.new ThreadTest();
    	te50.start();
    	*/
    	/*
    	ThreadTest te51 = s.new ThreadTest();
    	te51.start();
    	ThreadTest te52 = s.new ThreadTest();
    	te52.start();
    	ThreadTest te53 = s.new ThreadTest();
    	te53.start();
    	ThreadTest te54 = s.new ThreadTest();
    	te54.start();
    	ThreadTest te55 = s.new ThreadTest();
    	te55.start();
    	ThreadTest te56 = s.new ThreadTest();
    	te56.start();
    	ThreadTest te57 = s.new ThreadTest();
    	te57.start();
    	ThreadTest te58 = s.new ThreadTest();
    	te58.start();
    	ThreadTest te59 = s.new ThreadTest();
    	te59.start();
    	ThreadTest te60 = s.new ThreadTest();
    	te60.start();
    	
    	ThreadTest te61 = s.new ThreadTest();
    	te61.start();
    	ThreadTest te62 = s.new ThreadTest();
    	te62.start();
    	ThreadTest te63 = s.new ThreadTest();
    	te63.start();
    	ThreadTest te64 = s.new ThreadTest();
    	te64.start();
    	ThreadTest te65 = s.new ThreadTest();
    	te65.start();
    	ThreadTest te66 = s.new ThreadTest();
    	te66.start();
    	ThreadTest te67 = s.new ThreadTest();
    	te67.start();
    	ThreadTest te68 = s.new ThreadTest();
    	te68.start();
    	ThreadTest te69 = s.new ThreadTest();
    	te69.start();
    	ThreadTest te70 = s.new ThreadTest();
    	te70.start();
    	*/
//    	try {
//			Thread.sleep(50000);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    }
    	
    
    public void test1(){
    	    	Socket socket=null;
    	    	try {
//    				socket = new Socket("127.0.0.1",8888);
    	    		socket = new Socket("17.82.56.36",10005);
    				InputStream is = socket.getInputStream();
    				OutputStream os = socket.getOutputStream();
    				socket.setSoTimeout(1000);
    				
    				
//    				String s = "c012325228'|8801820150088";
    				String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20230227</sndDt><sndTm>105705</sndTm><seqNb>0227105705424876</seqNb><msgCd>CMIS.00220090.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><prdCode>010011</prdCode></request></body></transaction>";
    				os.write(s.getBytes("utf-8"));
    				try {
    					Thread.sleep(1000);
    				} catch (InterruptedException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    				byte[] b = new byte[1024];
    				is.read(b);
    				System.out.println(new String(b,"utf-8").trim());
    			} catch (UnknownHostException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}finally{
    				if(socket!=null){
    					try {
    						socket.close();
    					} catch (IOException e) {
    						// TODO Auto-generated catch block
    						e.printStackTrace();
    					}
    				}
    			}
    	    
    }
    
    class ThreadTest extends Thread{
    	
    	public void run(){
    		Random r = new Random();
    		int seq=r.nextInt(100000000);
//        	String s = "00000975<?xml version=\"1.0\" encoding=\"utf-8\" ?><transaction><header><msg><sndAppCd>MBSP</sndAppCd><sndDt>20180623</sndDt><sndTm>074259</sndTm><seqNb>1000000000684386</seqNb><msgCd>CMS.000030010.01</msgCd><callTyp>SYN</callTyp><rcvAppCd>CMS</rcvAppCd></msg></header><body><request><CbsReqHdr><transBranch>88078</transBranch><transDate>20180623</transDate><transTime>074259</transTime><transTlr>0939</transTlr><terminNo>1111</terminNo><transCode>3001</transCode><sndBrNo>88078</sndBrNo><sndDate>20180623</sndDate><opeNo>0939</opeNo></CbsReqHdr><mainInfo><chnlType>CMS</chnlType><cType>1</cType><tType>N</tType><idType>1</idType><idNo>13050219840731151X</idNo><cName>李冉</cName><eminAmt>1000</eminAmt><emaxAmt>250000.00</emaxAmt><begDate>20180620</begDate><endDate>20190620</endDate><tmpstr1>88078</tmpstr1><tmpstr2>000012</tmpstr2><dmaxAmt>0</dmaxAmt><mmaxAmt>0</mmaxAmt><qmaxAmt>0</qmaxAmt><ymaxAmt>0</ymaxAmt><tmpstr3/><tmpstr4/><tmpstr5/></mainInfo></request></body></transaction>";
//    		String s = "<?xml version=\"1.0\" encoding=\"utf-8\" ?><head><SYS_ID>MBSP</SYS_ID><T_DATE>20180623</T_DATE><T_TIME>"+seq+"</T_TIME><T_ID>YC0001</T_ID><T_SERNO>"+seq+"</T_SERNO></head><body><CERT_TYPE>10</CERT_TYPE><CERT_CODE>130523199106281675</CERT_CODE></body>";
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\" ?><transaction><header><ver></ver><msg><sndAppCd>CMIS</sndAppCd><sndDt>20190510</sndDt><sndTm>194617767</sndTm><seqNb>"+seq+"</seqNb><msgCd>CBS.200000040.01</msgCd><callTyp>SYN</callTyp><rcvAppCd>CBS</rcvAppCd></msg></header><body><request><ReqBizHdr><ChnlCD>CMIS</ChnlCD><InsNo>4110</InsNo><TlrNo>hx16</TlrNo><LglPsnID>001</LglPsnID><LglPsnInsNo>1</LglPsnInsNo><ExtSvcCD>CBS.200000040.01</ExtSvcCD><TrdPtTm>20190513</TrdPtTm><ExtStmDt>20190513</ExtStmDt><TraRefNo>CMIS20190513"+seq+"</TraRefNo><StmJrnlNo>"+seq+"</StmJrnlNo><StmID>CMIS</StmID></ReqBizHdr><CustNo>5700079511</CustNo><PgCD>1</PgCD><EnqrNum>999</EnqrNum><EnqrTp>3</EnqrTp></request></body></transaction>";
//   		String s="<?xml version=\"1.0\" encoding=\"UTF-8\" ?><?xml version=\"1.0\" encoding=\"UTF-8\" ?><transaction><header><msg><sndAppCd>CMIS</sndAppCd><sndDt>20190521</sndDt><sndTm>193631151</sndTm><seqNb>102019052100001053</seqNb><msgCd>ECIF.00013180.01</msgCd><callTyp>SYN</callTyp><rcvAppCd>ECIF</rcvAppCd></msg></header><body><request><ecifReqHdr><reqTe>0330</reqTe><reqOrg>88058</reqOrg></ecifReqHdr><custName>0715对公测试客户1</custName><certType>110</certType><certNo>45010119890401001X</certNo><mobileNo></mobileNo><acctNo></acctNo></request></body></transaction>";
    		
    		//质押贷测试报文
    		//产品查询
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20230227</sndDt><sndTm>023859</sndTm><seqNb>0227023859438573</seqNb><msgCd>CMIS.00220090.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><prdCode>010011</prdCode></request></body></transaction>";
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20201230</sndDt><sndTm>143120</sndTm><seqNb>1230143120789873</seqNb><msgCd>CMIS.00220120.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><acNo>880180120000000312</acNo><fPrdCode>-1</fPrdCode><depositSerno>1</depositSerno><collateralType>D</collateralType></request></body></transaction>";
    		
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20191102</sndDt><sndTm>135229</sndTm><seqNb>1102135229880047</seqNb><msgCd>CMIS.00220100.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWOUT</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><contNo>880382019110500066</contNo><flag>MBSP</flag><IfMobile>1</IfMobile><loanAmt>300000.00</loanAmt><loanStartDate>20191102</loanStartDate><loanEndDate>20191120</loanEndDate></request></body></transaction>";
    		//业务申请报文
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20200321</sndDt><sndTm>171511</sndTm><seqNb>0321171511379867</seqNb><msgCd>CMIS.00220050.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><org>88068</org><opId>9088</opId><appLmt>45000.00</appLmt><prdCode>010012</prdCode><idNo>132235197108112324</idNo><idType>10100</idType><name>刘一花</name><repaymentMode>CL17</repaymentMode><interestAccMode>03</interestAccMode><interestAccNum>1</interestAccNum><loanStartDate>20200321</loanStartDate><loanEndDate>20191104</loanEndDate><depositName>-1</depositName><cardNo>-1</cardNo><depositSerno>1</depositSerno><IfPeper>0</IfPeper><IfMobile>1</IfMobile><WMPType>1</WMPType><WMPPrdCode>grlc20191122</WMPPrdCode><WMPNo>6205283000100000097</WMPNo><phone>15922950366</phone><address>北京市北京市东城区在做一次了</address><transferAccount>6205283000100000097</transferAccount><transferAccName>刘一花</transferAccName><acNo>1</acNo><payBalAcNo>6205283000100000097</payBalAcNo><acNoP>1</acNoP><payBalAcName>刘一花</payBalAcName><flag>MBSP</flag><WMPStartDate>20191102</WMPStartDate><WMPEndDate>20191104</WMPEndDate><depositStartDate>notExit</depositStartDate><depositEndDate>notExit</depositEndDate><depositAmt>-1.00</depositAmt><WMPAmt>50000.00</WMPAmt><cusId>p049716914</cusId><WMPPrdName>专项测试20191122</WMPPrdName><depositPrdName>notExit</depositPrdName></request></body></transaction>";
    		
    		//放款报文,shouji
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><msg><sndAppCd>MBSP</sndAppCd><sndDt>20190806</sndDt><sndTm>101957</sndTm><seqNb>0806101957051609</seqNb><msgCd>CMIS.00220100.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><contNo>5486247</contNo><flag>MBSP</flag><IfMobile>1</IfMobile><loanAmt>5000.00</loanAmt><loanStartDate>20190801</loanStartDate><loanEndDate>20210801</loanEndDate></request></body></transaction>";
    		//学易贷审批进度查询
//    		String s= "<?xml version=\"1.0\" encoding=\"UTF-8\" ?><transaction><header><msg><sndAppCd>MBSP</sndAppCd><sndDt>20190708</sndDt><sndTm>154917497</sndTm><seqNb>102019062600003241</seqNb><msgCd>CMIS.00220070.01</msgCd><callTyp>SYN</callTyp><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><flag>MBSP</flag><cusId>p30000664</cusId><PrdCode>01014730</PrdCode></request></body></transaction>";
    		
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20190930</sndDt><sndTm>182102</sndTm><seqNb>0930182102557236</seqNb><msgCd>CMIS.00220110.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><contNo>881182019100920213</contNo><contAdress>//cont//</contAdress><fileName>sign881182019100920213.pdf_201909300440101.pdf</fileName></request></body></transaction>";

    		//负载均衡健康检查
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><msg><msgCd>ESB.00000010.01</msgCd><seqNb>123456789012345</seqNb><sndTm>090909999</sndTm><sndDt>20130909</sndDt><sndAppCd>ESB</sndAppCd></msg></header></transaction>";
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><msg><msgCd>ESB.00000010.01</msgCd><seqNb>123456789012345</seqNb><sndTm>090909999</sndTm><sndDt>20130909</sndDt><sndAppCd>ESB</sndAppCd></msg></header></transaction>";
    		
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20191219</sndDt><sndTm>105614</sndTm><seqNb>1219105614830285</seqNb><msgCd>CMIS.00220100.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><contNo>850582019101221014</contNo><flag>MBSP</flag><IfMobile>1</IfMobile><loanAmt>10000.00</loanAmt><loanStartDate>20191219</loanStartDate><loanEndDate>20191220</loanEndDate></request></body></transaction>";
    		
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20191221</sndDt><sndTm>123816</sndTm><seqNb>1221123816449186</seqNb><msgCd>CMIS.00220100.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><contNo>880882019101221218</contNo><flag>MBSP</flag><IfMobile>1</IfMobile><loanAmt>45000.00</loanAmt><loanStartDate>20191221</loanStartDate><loanEndDate>20191224</loanEndDate></request></body></transaction>";
    		//账户查询
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\" ?><transaction><header><ver></ver><msg><callTyp>SYN</callTyp><rcvAppCd>CBS</rcvAppCd><msgCd>CBS.200000040.01</msgCd><seqNb>201912220000006224</seqNb><sndTm>20191222</sndTm><sndDt>20191222</sndDt><sndAppCd>CMIS</sndAppCd></msg></header><body><request><EnqrTp>1</EnqrTp><CustNo></CustNo><TraacNo></TraacNo><ReqBizHdr><TrdPtTm>20191222</TrdPtTm><ChnlCD>0401001</ChnlCD><StmJrnlNo>201912220000006224</StmJrnlNo><LglPsnID>001</LglPsnID><TraRefNo>201912220000006224</TraRefNo><StmID>CMIS</StmID><ExtStmDt>20191218</ExtStmDt><LglPsnInsNo>1</LglPsnInsNo><TlrNo>0232</TlrNo><ExtSvcCD>CBS.200000040.01</ExtSvcCD><InsNo>88028</InsNo></ReqBizHdr><EnqrNum>1</EnqrNum><AccTp></AccTp><PgCD>1</PgCD></request></body></transaction>";
    		
    		//客户贷款查询
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20250710</sndDt><sndTm>173909</sndTm><seqNb>e004146504</seqNb><msgCd>LOAN.10000510.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>LOAN</rcvAppCd></msg></header><body><request><msgbody><serviceId>serv10000100051</serviceId><LOAN_NO>8806820130059-1</LOAN_NO></msgbody></request></body></transaction>";
//			个人客户信息同步
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00010010.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><cusId>p007673586</cusId><cusName>李红山</cusName><indivSex>1</indivSex><indivIdExpDt>2038-01-29</indivIdExpDt><indivNtn>01</indivNtn><indivDtOfBirth>1984-04-04</indivDtOfBirth><postAddr>河北省沙河市桥东办事处西杜村五区40号</postAddr><indivOcc>40000</indivOcc><unitName>鑫晶加油站</unitName><unitFld>F0000</unitFld><indivAnnIncm>1500.00</indivAnnIncm><country>CHN</country><mobile>15097969388</mobile></request></body></transaction>";

    		//基准利率转LPR业务查询
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00300010.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><certTyp>10100</certTyp><certCode>130503198706290049</certCode></request></body></transaction>";
    		
    		//基准利率转LPR业务新增
    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>MBSP</sndAppCd><sndDt>20200520</sndDt><sndTm>115313</sndTm><seqNb>0520115313292663</seqNb><msgCd>CMIS.00300020.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><billNo>8867820170171-1</billNo><realityIrY>0.047499996</realityIrY><lprPrimeRtKnd>1</lprPrimeRtKnd><baseLprRtKnd>LPR1</baseLprRtKnd><lprRulingIr>0.038500000</lprRulingIr><fixIntSprd>90.000000000</fixIntSprd><filePath>//cont//</filePath><fileName>lpr.pdf_20200520115315</fileName><channel>MBSP</channel></request></body></transaction>";
  
    		//共同借款人查询
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00300030.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><coCertTyp>10100</coCertTyp><coCertCode>133030196902260117</coCertCode></request></body></transaction>";
    		//共同借款人同意
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00300040.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><coCertTyp>10100</coCertTyp><coCertCode>130582198911013062</coCertCode><billNo>8803820120195-1</billNo><coBorrowerStatue>Y</coBorrowerStatue><filePath>SS</filePath><fileName>FF</fileName><channel>MBSP</channel></request></body></transaction>";
    		
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00300040.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><coCertTyp>10100</coCertTyp><coCertCode>133030196902260117</coCertCode><billNo>8803820120195-1</billNo><coBorrowerStatue>Y</coBorrowerStatue><filePath>SS</filePath><fileName>FF</fileName><channel>MBSP</channel></request></body></transaction>";
    		
    		//主借人撤销
//    		String s="<?xml version=\"1.0\" encoding=\"UTF-8\"?><transaction><header><ver>1.0</ver><msg><sndAppCd>ECIF</sndAppCd><sndDt>20200305</sndDt><sndTm>092627</sndTm><seqNb>e009185468</seqNb><msgCd>CMIS.00300050.01</msgCd><callTyp>SYN</callTyp><replyToQ>REP.TCP.GWIN</replyToQ><rcvAppCd>CMIS</rcvAppCd></msg></header><body><request><certTyp>10100</certTyp><certCode>130582197904141213</certCode><billNo>8803820120191-1</billNo><channel>MBSP</channel></request></body></transaction>";
    		String headerLength="";
			try {
				headerLength = s.getBytes("utf-8").length+"";
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			int i=headerLength.length();
			while(i<8){
				headerLength="0"+headerLength;
				i++;
			}
			s=headerLength+s;
    		Socket socket=null;
        	try {        		
//        		socket = new Socket("172.16.193.13",30014);
//        		socket = new Socket("16.83.15.136",10005);
//        		socket = new Socket("17.85.10.37",10005);
//        		socket = new Socket("17.82.53.68",30014);
        		socket = new Socket("localhost",10005);
//        		socket = new Socket("17.82.54.133",10005);
//        		socket = new Socket("16.83.15.136",10005);
    			InputStream is = socket.getInputStream();
    			OutputStream os = socket.getOutputStream();
    			socket.setSoTimeout(10000);
    			os.write(s.getBytes("utf-8"));
    			os.flush();
    			System.out.println(s);
    			byte[] ins;
    			while(true){
    				if(is.available()>0){
    					ins=new byte[is.available()];
    					is.read(ins);
    					break;
    				}
    				
    			}
    			
    			System.out.println(new String(ins,"utf-8").trim());
    		} catch (UnknownHostException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally{
    			if(socket!=null){
    				try {
    					socket.close();
    				} catch (IOException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
    			}
    		}
        	
    	}
    }
}
