package com.quickserverlab.quickcached.protocol;

import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.logging.Level;
import java.util.logging.Logger;
import junit.framework.TestCase;

/**
 *
 * @author akshath
 */
public class UDPTest extends TestCase {
	private DatagramSocket clientSocket = null;
	private InetAddress ipAddress = null;

	public void setUp() {
		try {
			clientSocket = new DatagramSocket();
			ipAddress =	InetAddress.getByName("127.0.0.1");
			
		} catch (Exception ex) {
			Logger.getLogger(BinaryProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}

	public void tearDown() {
		if (clientSocket != null) {
			clientSocket.close();
		}
	}

	public UDPTest(String name) {
		super(name);
	}

	public void testGet() {
		try {
			byte[] sendData = new byte[1024];
			byte[] receiveData = new byte[1024];
			
			String header = new String(decodeToByte("0045"+"0000"+"0001"+"0000"), "ISO-8859-1");
			System.out.println("header:"	+ header.length());

							
			String data = header +"stats\r\n";
			System.out.println("TO SERVER:"	+ data);
			sendData = data.getBytes("ISO-8859-1");

			DatagramPacket sendPacket =	new DatagramPacket(sendData, sendData.length,
				ipAddress, 11211);
			clientSocket.send(sendPacket);
			
			DatagramPacket receivePacket =	new DatagramPacket(receiveData,
				receiveData.length);
			clientSocket.receive(receivePacket);
			
			String modifiedSentence =
				new String(receivePacket.getData());

			System.out.println("FROM SERVER:"
				+ modifiedSentence);
		} catch (Exception ex) {
			Logger.getLogger(BinaryProtocolTest.class.getName()).log(Level.SEVERE, null, ex);
		}
	}
	
	public static byte[] decodeToByte(String hexText) {
		String chunk = null;
		if (hexText != null && hexText.length() > 0) {
			int numBytes = hexText.length() / 2;

			byte[] rawToByte = new byte[numBytes];
			int offset = 0;
			for (int i = 0; i < numBytes; i++) {
				chunk = hexText.substring(offset, offset + 2);
				offset += 2;
				rawToByte[i] = (byte) (Integer.parseInt(chunk, 16) & 0x000000FF);
			}
			return rawToByte;
		}
		return null;
	}
	
	public static String encode(String sourceText) throws UnsupportedEncodingException {
		return encode(sourceText.getBytes("ISO-8859-1"));
	}
	public static String encode(byte[] rawData) {
		StringBuilder hexText = new StringBuilder();
		String initialHex = null;
		int initHexLength = 0;

		for (int i = 0; i < rawData.length; i++) {
			int positiveValue = rawData[i] & 0x000000FF;
			initialHex = Integer.toHexString(positiveValue);
			initHexLength = initialHex.length();
			while (initHexLength++ < 2) {
				hexText.append("0");
			}
			hexText.append(initialHex);
		}
		return hexText.toString();
	}
}
