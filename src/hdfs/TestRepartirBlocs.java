package hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class TestRepartirBlocs {

	public static void main(String[] args) {
		LinkedHashMap<String, Integer> availableServers = new LinkedHashMap<String, Integer>();
		availableServers.put("bore", 8090);
		availableServers.put("carbone", 8090);
		availableServers.put("luke", 8090);
		
		HashMap<Integer, ArrayList<String>> repartition = HdfsUtil.repartirBlocs(availableServers,
				2, 5);
		
		/* afficher la hashmap résultat */
		HdfsUtil.printHashMap(repartition);

	}

}
