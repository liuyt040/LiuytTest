package ncmis.test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class SocketServerTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

		try {
			ServerSocket s = new ServerSocket(1521);
			System.out.println("端口启动");
			Socket socket=s.accept();
			InputStream in = socket.getInputStream();
			OutputStream out = socket.getOutputStream();
			int count=1;
			while(true){
				System.out.println("第"+count+"次");
				if(in.available()>0){
					byte[] b = new byte[in.available()];
					in.read(b);
					System.out.println(new String(b,"utf-8"));
					out.write("服务端接收成功!".getBytes("utf-8"));
					out.flush();
					break;
				}
				
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
