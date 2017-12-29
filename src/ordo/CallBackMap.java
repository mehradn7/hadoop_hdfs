package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.CountDownLatch;

public class CallBackMap extends UnicastRemoteObject implements CallBack {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private CountDownLatch barrier;

	protected CallBackMap(CountDownLatch b) throws RemoteException {
		super();
		this.barrier = b;
	}

	@Override
	public void wakeUp() {
		this.barrier.countDown();
	}

}