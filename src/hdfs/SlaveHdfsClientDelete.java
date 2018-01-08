package hdfs;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class SlaveHdfsClientDelete extends Thread {
	
	private Socket s;
	private String hdfsFname;
	
	public SlaveHdfsClientDelete(String host, int port, String hdfsFname) 
			throws UnknownHostException, IOException {
		this.s = new Socket(host, port);
		this.hdfsFname = hdfsFname;
	}
	
	public void run(){
		try{
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("delete");
			oos.writeObject(this.hdfsFname);
		}catch(IOException e1) {
			e1.printStackTrace();
		}
	}
}
