package formats;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

public class SocketFormat implements Format {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private long index;
	private Socket s;
	private int port;
	private String host;
	private ObjectOutputStream oos;
	private ObjectInputStream ois;
	private Collection<KV> kvs;
	
	public SocketFormat(String host, int port, Collection<KV> kvs){
		this.index = 1L;
		this.host = host;
		this.port = port;
		this.kvs = kvs;
	}

	@Override
	public KV read() {
		String res;
		KV kv = null;
		try {
			kv = (KV) this.ois.readObject();
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}
		this.index++;
		return kv;
	}

	@Override
	public void write(KV record) {
		try{
			this.oos.writeObject(record);
		}catch(IOException e){
			e.printStackTrace();
		}
		this.index++;
	}

	@Override
	public void open(OpenMode mode) {
		this.index = 1L;
		if (mode == OpenMode.R){
			try {
				this.s = new Socket(this.host, this.port);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}else{
			try {
				this.s = new Socket(this.host, this.port);
			} catch (IOException e) {
				e.printStackTrace();
			}	
		}
	}

	@Override
	public void close() {
		try {
			this.s.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public long getIndex() {
		return this.index;
	}

	@Override
	public String getFname() {
		return this.host;
	}

	@Override
	public void setFname(String host) {
		this.host = host;
	}
}
