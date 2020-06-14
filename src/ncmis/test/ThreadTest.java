package ncmis.test;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.SecureRandom;

public class ThreadTest extends Thread {

	private  SecureRandom seeder = new SecureRandom();
	public String getUNID() {
		StringBuffer buf = new StringBuffer();
		long time = System.currentTimeMillis(); //1588728636950
		int timeLow = (int) time & -1;//-409262570
		int node = seeder.nextInt(); //-1207495116
		String midString = null;
		try {
			InetAddress inet = InetAddress.getLocalHost();//vie-PC/17.85.3.153
			byte bytes[] = inet.getAddress();
			String hexAddress = hexFormat(getInt(bytes), 8);//[17, 85, 3, -103]
			String hash = hexFormat(System.identityHashCode(this), 8);//01E97F9F
			midString = (new StringBuilder()).append(hexAddress).append(hash)//000164C901E97F9F
					.toString();
		} catch (UnknownHostException e) {
		}
		if (midString == null)
			midString = "0000000000000000";
		buf.append(midString).append(hexFormat(timeLow, 8))
				.append(hexFormat(node, 8));
		return buf.toString();//000164C901E97F9FE79B2616B8071634
	}

	private  String hexFormat(int number, int digits) {
		String hex = Integer.toHexString(number).toUpperCase();
		if (hex.length() >= digits)
			return hex.substring(0, digits);
		int padding = digits - hex.length();
		StringBuffer buf = new StringBuffer();
		for (int i = 0; i < padding; i++)
			buf.append("0");

		buf.append(hex);
		return buf.toString();
	}

	private  int getInt(byte bytes[]) {
		int size = bytes.length <= 32 ? bytes.length : 32;
		int result = 0;
		for (int i = size - 1; i >= 0; i--)
			if (i == size - 1)
				result += bytes[i];
			else
				result += bytes[i] << 4 * (size - 1 - i);

		return result;
	}
}
