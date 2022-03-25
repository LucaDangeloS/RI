package udc.rigrado;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;

public class SimpleReader {

	/**
	 * Project testlucene9_0_0 SimpleReader1 class reads the index SimpleIndex
	 * created with the SimpleIndexing class
	 */
	public static void main(String[] args) {

		if (args.length != 1) {
			System.out.println("Usage: java SimpleReader SimpleIndex");
			return;
		}
		// SimpleIndex is the folder where the index SimpleIndex is stored

		String indexFolder = args[0];

		Directory dir = null;
		DirectoryReader indexReader = null;
		Document doc = null;

		List<IndexableField> fields = null;

		try {
			dir = FSDirectory.open(Paths.get(indexFolder));
			indexReader = DirectoryReader.open(dir);
		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		for (int i = 0; i < indexReader.numDocs(); i++) {

			try {
				doc = indexReader.document(i);
			} catch (CorruptIndexException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			}
			System.out.println("Documento " + i);
			System.out.println("path = " + doc.get("path"));
			System.out.println("modified = " + doc.get("modified"));
			System.out.println("creationTime = " + doc.get("creationTime"));
			System.out.println("lastAccessTime = " + doc.get("lastAccessTime"));
			System.out.println("lastModifiedTime = " + doc.get("lastModifiedTime"));
			System.out.println("creationTimeLucene = " + doc.get("creationTimeLucene"));
			System.out.println("lastAccessTimeLucene = " + doc.get("lastAccessTimeLucene"));
			System.out.println("lastModifiedTimeLucene = " + doc.get("lastModifiedTimeLucene"));
			System.out.println("contents = " + doc.get("contents"));
//			System.out.println("contentsStored = " + doc.get("contentsStored"));
			System.out.println("hostname = " + doc.get("hostname"));
			System.out.println("thread = " + doc.get("thread"));
			System.out.println("sizeKB = " + doc.get("sizeKBStored"));
			System.out.println("onlyTopLines = " + doc.get("onlyTopLines"));
			System.out.println("onlyBottomLines = " + doc.get("onlyBottomLines"));

		}
		System.exit(0);
		/**
		 * Note doc.get() returns null for the fields that were not stored
		 */

		for (int i = 0; i < indexReader.numDocs(); i++) {

			try {
				doc = indexReader.document(i);
			} catch (CorruptIndexException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			} catch (IOException e1) {
				System.out.println("Graceful message: exception " + e1);
				e1.printStackTrace();
			}

			System.out.println("Documento " + i);

			fields = doc.getFields();
			// Note doc.getFields() gets the stored fields

			for (IndexableField field : fields) {
				String fieldName = field.name();
				System.out.println(fieldName); //  + ": " + doc.get(fieldName)

			}

		}

		final Term term = new Term("modelDescription", "model");
		try {
			System.out.println("\nTerm(field=modelDescription, text=model)" + "doc frequency= "
					+ indexReader.docFreq(term) + "total term frequency= " + indexReader.totalTermFreq(term));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		IndexSearcher searcher = null;
		searcher = new IndexSearcher(indexReader);

		CollectionStatistics modelDescriptionStatistics = null;
		try {
			modelDescriptionStatistics = searcher.collectionStatistics("modelDescription");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		System.out.println("\nmodel Description Statistics\n");
		System.out.println("\nfield= " + modelDescriptionStatistics.field() + " docCount= "
				+ modelDescriptionStatistics.docCount() + " maxDoc= " + modelDescriptionStatistics.maxDoc()
				+ " sumDocFreq= " + modelDescriptionStatistics.sumDocFreq() + " sumTotalFreq= "
				+ modelDescriptionStatistics.sumTotalTermFreq());

		try {
			indexReader.close();
			dir.close();
		} catch (IOException e) {
			System.out.println("Graceful message: exception " + e);
			e.printStackTrace();
		}

	}
}