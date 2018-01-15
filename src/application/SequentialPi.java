package application;

import java.util.HashMap;
import java.util.Map;

import formats.Format.OpenMode;
import formats.KV;
import formats.KvFormat;
import formats.LineFormat;

public class SequentialPi {
	public static void main(String[] args) {

		GenerateHaltonFile.generateFile(args[0], Integer.parseInt(args[1]));
		long t1 = System.currentTimeMillis();

		calculatePi(args[0]);

		long t2 = System.currentTimeMillis();
		System.out.println("[PiSequential] time in ms =" + (t2 - t1));
		System.exit(0);

	}

	private static void calculatePi(String filepath) {
		KV kv;

		Map<String, Integer> hm = new HashMap<>();

		LineFormat sourceFile = new LineFormat(filepath);
		KvFormat postMap = new KvFormat(filepath + "-mapped");

		sourceFile.open(OpenMode.R);

		/* Pseudo map */

		while ((kv = sourceFile.read()) != null) {
			/* Le point est dans kv.v */
			if (PiMCMapReduce.isInCircle(kv.v)) {
				hm.put(kv.v, 1);
			} else {
				hm.put(kv.v, 0);
			}

		}
		postMap.open(OpenMode.W);

		for (String k : hm.keySet()) {
			postMap.write(new KV(k, hm.get(k).toString()));
		}

		/* Pseudo reduce */
		postMap.open(OpenMode.R);
		KV kv2;
		int nbPointsInCircle = 0;
		int nbPoints = 0;

		while ((kv2 = postMap.read()) != null) {
			if (Integer.parseInt(kv2.v) == 1) {
				nbPointsInCircle++;
			}
			nbPoints++;
		}
		double pi = 16 * ((double) nbPointsInCircle / nbPoints);
		System.out.println("here");
		KvFormat postReduce = new KvFormat(filepath + "-reduced");
		postReduce.open(OpenMode.W);
		postReduce.write(new KV("pi", Double.toString(pi)));
	}

}
