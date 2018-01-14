package application;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import formats.KV;

public class GenerateHaltonFile {

	public static void generateFile(String filename, int nbPoints) {
		try {
			HaltonSequence halton = new HaltonSequence(0);
			File f = new File(filename);
			FileWriter fr = new FileWriter(f);
			double[] point;
			for (int i = 0; i < nbPoints; i++) {
				point = halton.nextPoint();
				fr.write(point[0] + "@" + point[1] + "\n");

			}
			fr.close();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		if (!(args.length == 2)) {
			System.out.println("java GenerateHaltonFile filename nbPoints");
			return;
		}
		
		GenerateHaltonFile.generateFile(args[0], Integer.parseInt(args[1]));

		/*BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(new File("halton.txt")));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		String s = "";
		try {
			s = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String[] split = s.split(KV.SEPARATOR);
		System.out.println(split[0] + " ... " + split[1]);*/
		

	}

}
