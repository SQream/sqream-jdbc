package com.sqream.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class Utils {

	static int ConvertToInt(byte[] value) {
		ByteBuffer wrapped = ByteBuffer.wrap(value);
		wrapped.order(ByteOrder.LITTLE_ENDIAN);
		return wrapped.getInt();
	}
	static Tuple <String,Integer> getLBConnection(String host,int port) throws IOException
	{
		
		       	 
				// reconnect to picker
				Socket socketClient = new Socket(host, port);
				InputStream is = socketClient.getInputStream();
				// get input
				byte[] b = new byte[4];
				socketClient.getInputStream().read(b, 0, 4);

				int size = ConvertToInt(b);

				byte[] ip = new byte[size];
				socketClient.getInputStream().read(ip, 0, size);
				

				byte[] b_port = new byte[4];
				socketClient.getInputStream().read(b_port, 0, 4);
                String ipaddress = new String(ip);
				
				

				is.close();
				socketClient.close();
				
				
				return  new Tuple <String,Integer>(ipaddress,ConvertToInt(b_port));
			
				
		
	}
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
