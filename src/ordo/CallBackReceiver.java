package ordo;

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.concurrent.CountDownLatch;

public class CallBackReceiver extends UnicastRemoteObject implements ICallBack {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private CountDownLatch barrier;
	
	protected CallBackReceiver(CountDownLatch b) throws RemoteException {
		super();
		this.barrier = b;
	}
	
	@Override
	public void isTerminated() throws RemoteException {
		this.barrier.countDown();
	}

}
