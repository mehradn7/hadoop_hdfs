package hdfs;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class INode implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String filename;
	private int repFactor;
	private HashMap<Integer, ArrayList<String>> mapBlocs;

	public INode(String filename){
		this.filename = filename;
	}
	public INode(String filename, int repFactor) {
		this.filename = filename;
		this.repFactor = repFactor;
		this.mapBlocs = new HashMap<Integer, ArrayList<String>>();
	}
	
	public INode(String filename, int repFactor, HashMap<Integer, ArrayList<String>> mapBlocs) {
		this.filename = filename;
		this.repFactor = repFactor;
		this.mapBlocs = mapBlocs;
	}

	public int getRepFactor() {
		return repFactor;
	}

	public void setRepFactor(int repFactor) {
		this.repFactor = repFactor;
	}

	public String getFilename() {
		return filename;
	}

	public void setFilename(String filename) {
		this.filename = filename;
	}

	public HashMap<Integer, ArrayList<String>> getMapBlocs() {
		return mapBlocs;
	}
	
	public void setMapBlocs(HashMap<Integer, ArrayList<String>> mapBlocs) {
		this.mapBlocs = mapBlocs;
	}

}
