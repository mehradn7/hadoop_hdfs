package application;

import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import formats.Format;
import formats.FormatReader;
import formats.FormatWriter;
import formats.KV;
import map.MapReduce;
import ordo.Job;

public class PiMCMapReduce implements MapReduce {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public void map(FormatReader reader, FormatWriter writer) {

		Map<String, Integer> hm = new HashMap<>();
		KV kv;
		while ((kv = reader.read()) != null) {
			/* Le point est dans kv.v */
			if (isInCircle(kv.v)) {
				hm.put(kv.v, 1);
			} else {
				hm.put(kv.v, 0);
			}

		}
		for (String k : hm.keySet())
			writer.write(new KV(k, hm.get(k).toString()));
	}

	/**
	 * 
	 * @param v
	 *            le string qui représente le point
	 * @return vrai si le point est dans le cercle unité
	 */
	public static boolean isInCircle(String point) {
		String[] coordonnees = point.split("@");
		double x = Double.parseDouble(coordonnees[0]);
		double y = Double.parseDouble(coordonnees[1]);

		if (x * x + y * y > 0.25) {
			return false;
		} else {
			return true;
		}
	}

	@Override
	public void reduce(FormatReader reader, FormatWriter writer) {
		KV kv;
		int nbPointsInCircle = 0;
		int nbPoints = 0;
		while ((kv = reader.read()) != null) {
			if (Integer.parseInt(kv.v) == 1) {
				nbPointsInCircle ++;
			}
			nbPoints ++;
		}
		double pi = 16 * ((double) nbPointsInCircle / nbPoints);
		writer.write(new KV("pi", Double.toString(pi)));
	}

	public static void main(String[] args) {
		Job j = null;
		try {
			j = new Job();
		} catch (RemoteException e) {
			e.printStackTrace();
			System.exit(1);
		}
		GenerateHaltonFile.generateFile("../data/"+args[0], Integer.parseInt(args[1]));
		j.setInputFormat(Format.Type.LINE);
		j.setInputFname(args[0]);
		j.setOutputFname("pi-" + args[0]);
		j.setNumberOfReduces(1);
		long t1 = System.currentTimeMillis();
		j.startJob(new PiMCMapReduce());
		long t2 = System.currentTimeMillis();
		System.out.println("[PiMapReduce] time in ms =" + (t2 - t1));
		Count.main(args);
		System.exit(0);
	}

}
