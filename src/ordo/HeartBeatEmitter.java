package ordo;

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

public class HeartBeatEmitter extends Thread implements IHeartBeatEmitter {

	private Socket s;
	private OutputStream os;
	
	public HeartBeatEmitter(String hostname, int port) throws UnknownHostException, IOException {
		this.s = new Socket(hostname, port);
		this.os = s.getOutputStream();
	}
	
	@Override
	public void run() {
		try {
			while(true) {
				this.os.write(1);
				TimeUnit.SECONDS.sleep(1);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
