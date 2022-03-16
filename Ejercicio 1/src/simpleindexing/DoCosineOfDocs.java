package simpleindexing;

import java.io.IOException;

public class DoCosineOfDocs {

	/**
	 * @param args
	 * 
	 *             project testlucen8_1_1
	 * 
	 */
	public static void main(String[] args) {

		if (args.length != 1) {
			System.out.println("Usage: java DoCosineOfDocs indexFolder");
			return;
		}
		// SimpleIndex is the folder where the index SimpleIndex is stored

		String indexFolder = args[0];

		String d1 = "uno uno";
		String d2 = "uno dos uno dos";

		double c = 0;

		// Coseno(45º) = 0.7071

		try {
			c = CosineDocumentSimilarity.getCosineSimilarity(d1, d2, indexFolder);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println("Coseno de los docs");
		System.out.println("d1 = \"" + d1 + "\"");
		System.out.println("d2 = \"" + d2 + "\"");
		System.out.println("= " + c);
		System.out.println();

	}
}
