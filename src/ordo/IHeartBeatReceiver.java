package ordo;

import java.net.Socket;

public interface IHeartBeatReceiver extends Runnable {
	
	public void addEmitter(Socket emitter);
	public void removeEmitter(Socket emitter);
	public void run();
	public void updateLiving();
}
