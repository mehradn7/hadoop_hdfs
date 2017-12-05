package ordo;

import java.io.IOException;
import java.net.Socket;

public interface IHeartBeatReceiver extends Runnable {
	
	public void addEmitter(Socket emitter);
	public void removeEmitter(Socket emitter) throws IOException;
	public void run();
}
