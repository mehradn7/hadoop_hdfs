package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;

import formats.Format;
import map.Mapper;
import map.Reducer;

public interface IDaemon extends Remote {

	public String getLocalHostname() throws RemoteException;
	public void setLocalHostname(String hostname) throws RemoteException;
	public void runMap (Mapper mapper, Format reader, Format writer, ICallBack cb) 
			throws RemoteException;
	public void runReduce (Reducer reducer, Format reader, Format writer, ICallBack cb) 
			throws RemoteException;
}
