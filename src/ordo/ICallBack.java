package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface ICallBack extends Remote {
	
	public void isTerminated() throws RemoteException;
	
}
