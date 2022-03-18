package simpleindexing;

/*
 * projecto testlucene8_1_1
 * El main invoca dos threads, cada uno crea un índice MMapDirectory
 * El main espera que acaben los dos threads (con p1.join() y p2.join()
 * El main fusiona los dos índices en otro
 * El main busca en este índice fusionado
 * 
 * Alternativamente se podria hacer que los threads
 * indexen concurrentemente sobre el mismo indice sin necesidad de crear
 * indices distintos ya que el IndexWriter es Thread Safe
 */

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.MMapDirectory;;

class IndexThread extends Thread {
	String s;
	MMapDirectory dir;

	IndexThread(MMapDirectory dir, String s) {
		this.dir = dir;
		this.s = s;
	}

	public void run() {
		Analyzer analyzer = new StandardAnalyzer();

		// Store the index in memory:
		// To store an index on disk, use this instead:
		// Directory directory = FSDirectory.open("/tmp/testindex");
		IndexWriterConfig config = new IndexWriterConfig(analyzer);
		IndexWriter iwriter = null;
		try {
			iwriter = new IndexWriter(dir, config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Document doc = new Document();
		doc.add(new Field("fieldname", s, TextField.TYPE_STORED));
		try {
			iwriter.addDocument(doc);
			iwriter.commit();
			iwriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}

public class SimpleThread2 {

	/**
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws InterruptedException {

		String doc1 = "Este es el texto que indexa el primer thread";
		String doc2 = "Este es el texto que indexa el segundo thread";

		MMapDirectory dir1 = null;
		try {
			dir1 = new MMapDirectory(Paths.get("/tmp/LuceneIndex1/"));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}
		MMapDirectory dir2 = null;
		try {
			dir2 = new MMapDirectory(Paths.get("/tmp/LuceneIndex2/"));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		IndexThread p1 = new IndexThread((MMapDirectory) dir1, doc1);
		p1.start();
		IndexThread p2 = new IndexThread((MMapDirectory) dir2, doc2);
		p2.start();

		try {
			p1.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			p2.join();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		System.out.println();
		System.out.println("Finalizado thread " + p1.getName());

		System.out.println();
		System.out.println("Finalizado thread " + p2.getName());

		IndexWriterConfig iconfig = new IndexWriterConfig(new StandardAnalyzer());
		IndexWriter ifusedwriter = null;

		MMapDirectory dir3 = null;
		try {
			dir3 = new MMapDirectory(Paths.get("/tmp/LuceneIndex3/"));
		} catch (IOException e2) {
			// TODO Auto-generated catch block
			e2.printStackTrace();
		}

		try {
			ifusedwriter = new IndexWriter(dir3, iconfig);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			ifusedwriter.addIndexes(dir1, dir2);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			ifusedwriter.commit();
			ifusedwriter.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		DirectoryReader ifusedreader = null;
		try {
			ifusedreader = DirectoryReader.open(dir3);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		IndexSearcher isearcher = new IndexSearcher(ifusedreader);
		// Parse a simple query that searches for "text":
		QueryParser parser = new QueryParser("fieldname", new StandardAnalyzer());
		Query query = null;
		try {
			query = parser.parse("thread");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		ScoreDoc[] hits = null;
		try {
			hits = isearcher.search(query, 1000).scoreDocs;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		// Iterate through the results:

		for (int i = 0; i < hits.length; i++) {
			Document hitDoc = null;
			try {
				hitDoc = isearcher.doc(hits[i].doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			System.out.println(
					"This is the text of the document that was indexed and there is a hit for the query 'thread': "
							+ hitDoc.get("fieldname"));

		}
		try {
			dir1.close();
			dir2.close();
			ifusedreader.close();
			dir3.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}