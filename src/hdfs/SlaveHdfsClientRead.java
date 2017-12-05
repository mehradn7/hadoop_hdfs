package hdfs;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;

public class SlaveHdfsClientRead extends Thread {
	
	private Socket s;
	private String hdfsFname;
	private ObjectInputStream ois;

	public SlaveHdfsClientRead(String host, int port, String hdfsFname) 
			throws UnknownHostException, IOException {
		this.s = new Socket(host, port);
		this.hdfsFname = hdfsFname;
	}
	
	public void run(){
		try{
			ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
			oos.writeObject("read");
			oos.writeObject(this.hdfsFname);
			setObjectInputStream(new ObjectInputStream(s.getInputStream()));
		}catch(IOException e1) {
			e1.printStackTrace();
		}
	}

	public ObjectInputStream getObjectInputStream() {
		return ois;
	}

	public void setObjectInputStream(ObjectInputStream ois) {
		this.ois = ois;
	}
}

