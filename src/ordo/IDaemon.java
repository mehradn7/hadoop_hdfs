package ordo;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.HashMap;

import formats.Format;
import map.Mapper;
import map.Reducer;

public interface IDaemon extends Remote {

	public String getLocalHostname() throws RemoteException;
	
	public void setLocalHostname(String hostname) throws RemoteException;
	
	public HashMap<String, String> getKeyToDaemon() throws RemoteException;
	
	public void setKeyToDaemon(HashMap<String, String> keyToDaemon) throws RemoteException;
	
	public String getMapperFname() throws RemoteException;
	
	public void setMapperFname(String mapperFname) throws RemoteException;
	
	public void runMap (Mapper mapper, Format reader, Format writer, ICallBack cb) 
			throws RemoteException;
	
	public void runReducer (Reducer reducer, Format reader, Format writer, ICallBack cb) 
			throws RemoteException;
	
	public void runReceiver(Format writer, ICallBack callbackReceiver) throws RemoteException;
	
	public void runSender() throws RemoteException;
}
