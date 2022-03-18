package simpledemo;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

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
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class SimpleDemo2 {

	/**
	 * @param args
	 * @throws IOException
	 * @throws ParseException
	 */
	public static void main(String[] args) throws IOException, ParseException {
		Analyzer analyzer = new StandardAnalyzer();

		Path indexPath = Files.createTempDirectory("tempIndex");
		Directory directory = FSDirectory.open(indexPath);
		IndexWriterConfig config = new IndexWriterConfig(analyzer);

		float lambda = (float) 1.0;
		LMJelinekMercerSimilarity similarity = new LMJelinekMercerSimilarity(lambda);
		config.setSimilarity(similarity);

		IndexWriter iwriter = new IndexWriter(directory, config);

		Document doc1 = new Document();
		String text1 = "This is the text to be indexed.";
		doc1.add(new Field("fieldname", text1, TextField.TYPE_STORED));

		Document doc2 = new Document();
		String text2 = "This is document to be indexed.";
		doc2.add(new Field("fieldname", text2, TextField.TYPE_STORED));

		Document doc3 = new Document();
		String text3 = "This is document to be indexed.";
		doc3.add(new Field("fieldname", text3, TextField.TYPE_STORED));

		iwriter.addDocument(doc1);
		iwriter.addDocument(doc2);
		iwriter.addDocument(doc3);

		iwriter.close();

		// Now search the index:
		DirectoryReader ireader = DirectoryReader.open(directory);
		IndexSearcher isearcher = new IndexSearcher(ireader);
		// Parse a simple query that searches for "text":
		QueryParser parser = new QueryParser("fieldname", analyzer);
		Query query = parser.parse("text");
		ScoreDoc[] hits = isearcher.search(query, 1000).scoreDocs;
		assertEquals(1, hits.length);
		// Iterate through the results:
		for (int i = 0; i < hits.length; i++) {
			Document hitDoc = isearcher.doc(hits[i].doc);
			/**
			 * assertEquals("This is not the text that was indexed.",
			 * hitDoc.get("fieldname"));
			 */
			assertEquals("This is the text to be indexed.", hitDoc.get("fieldname"));
		}
		ireader.close();
		directory.close();
		// IOUtils.rm(indexPath);
	}

}
