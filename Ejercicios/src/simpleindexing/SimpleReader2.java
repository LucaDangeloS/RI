package simpleindexing;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class SimpleReader2 {

	/**
	 * Los índices de Lucene se almacenan en forma de segmentos, cada segmento se
	 * considera una hoja (leaf) del índice. Este ejemplo lee los contenidos de un
	 * índice usando AtomicReaders. Un AtomicReader lee los contenidos de un
	 * segmento o leaf.
	 * 
	 * IndexReader instances for indexes on disk are usually constructed with a call
	 * to one of the static DirectoryReader.open() methods, e.g.
	 * DirectoryReader.open(Directory). DirectoryReader implements the
	 * CompositeReader interface, it is not possible to directly get postings.
	 * 
	 * LeafReader: These indexes do not consist of several sub-readers, they are
	 * atomic. They support retrieval of stored fields, doc values, terms, and
	 * postings.
	 *
	 * Si nuestro índice es pequeño solamente tendrá un segmento por lo que la
	 * llamada indexReader.getContext().leaves() nos dará una lista con un único
	 * LeafReaderContext (objeto del cual se obtiene el LeafReader).
	 */
	public static void main(final String[] args) throws IOException {

		if (args.length != 2) {
			System.out.println("Usage: java SimpleReader2 <index_folder> <field_name>");
			return;
		}

		Directory dir = null;
		DirectoryReader indexReader = null;

		BufferedReader in;

		in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));

		try {
			dir = FSDirectory.open(Paths.get(args[0]));
			indexReader = DirectoryReader.open(dir);
		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		// Get the context for each leaf (segment)

		/**
		 * We can do for (final LeafReaderContext leaf :
		 * indexReader.getContext().leaves()) { or ---
		 */

		System.out.println("Size of  indexReader.leaves() = " + indexReader.leaves().size());

		for (final LeafReaderContext leaf : indexReader.leaves()) {
			// Print leaf number (starting from zero)
			System.out.println("We are in the leaf number " + leaf.ord);

			// Create an AtomicReader for each leaf
			// (using, again, Java 7 try-with-resources syntax)
			try (LeafReader leafReader = leaf.reader()) {

				String field = args[1];

				System.out.println("Field = " + field);
				final Terms terms = leafReader.terms(field);
				final TermsEnum termsEnum = terms.iterator();

				while (termsEnum.next() != null) {
					final String tt = termsEnum.term().utf8ToString();
					// totalFreq equals -1 if the value was not
					// stored in the codification of this index
					System.out.println("\t" + tt + "\ttotalFreq()=" + termsEnum.totalTermFreq() + "\tdocFreq="
							+ termsEnum.docFreq());

				}

				System.out.println("Enter input: ");

				try {
					in.readLine();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

				int doc;
				final Term term = new Term("modelDescription", "probability");
				final PostingsEnum postingsEnum = leafReader.postings(term);

				while ((doc = postingsEnum.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
					System.out.println("\nTerm(field=modelDescription, text=probability)" + " appears in doc num: "
							+ doc + " with term frequency= " + postingsEnum.freq());
					final Document d = leafReader.document(doc);
					System.out.println("modelDescription = " + d.get("modelDescription"));
				}

			}
		}

	}

}