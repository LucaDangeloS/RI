package simpleindexing;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

public class SimpleSearch {

	/**
	 * Project testlucene9_0_0 SimpleSearch class reads the index SimpleIndex
	 * created with the SimpleIndexing class, creates and Index Searcher and search
	 * for documents which contain the word "probability" in the field
	 * "modelDescription" using the StandardAnalyzer Also contains and example
	 * sorting the results by reverse document number (index order). Also contains
	 * an example of a boolean programmatic query
	 * 
	 */
	public static void main(String[] args) {

		if (args.length != 1) {
			System.out.println("Usage: java SimpleSearch SimpleIndex");
			return;
		}
		// SimpleIndex is the folder where the index SimpleIndex is stored

		IndexReader reader = null;
		Directory dir = null;
		IndexSearcher searcher = null;
		QueryParser parser;
		Query query = null;

		try {
			dir = FSDirectory.open(Paths.get(args[0]));
			reader = DirectoryReader.open(dir);

		} catch (CorruptIndexException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		searcher = new IndexSearcher(reader);
		parser = new QueryParser("modelDescription", new StandardAnalyzer());

		try {
			query = parser.parse("probability");
		} catch (ParseException e) {

			e.printStackTrace();
		}

		TopDocs topDocs = null;

		try {
			topDocs = searcher.search(query, 10);
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
		System.out.println(
				"\n" + topDocs.totalHits + " results for query \"" + query.toString() + "\" showing for the first " + 10
						+ " documents the doc id, score and the content of the modelDescription field");

		for (int i = 0; i < Math.min(10, topDocs.totalHits.value); i++) {
			try {
				System.out.println(topDocs.scoreDocs[i].doc + " -- score: " + topDocs.scoreDocs[i].score + " -- "
						+ reader.document(topDocs.scoreDocs[i].doc).get("modelDescription"));
			} catch (CorruptIndexException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			}

		}

		// follows and example sorting the results
		// by reverse document number (index order)

		boolean reverse = true;
		try {
			topDocs = searcher.search(query, 10,
					new Sort(new SortField("modelDescription", SortField.Type.DOC, reverse)));
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}
		System.out.println("\n" + topDocs.totalHits + " results for query \"" + query.toString()
				+ "\" in the sort given by reverse document number, " + "\" showing for the first " + 10
				+ " documents the doc id, score and the content of the modelDescription field");

		for (int i = 0; i < Math.min(10, topDocs.totalHits.value); i++) {
			try {
				System.out.println(topDocs.scoreDocs[i].doc + " -- score: " + topDocs.scoreDocs[i].score + " -- "
						+ reader.document(topDocs.scoreDocs[i].doc).get("modelDescription"));
			} catch (CorruptIndexException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			}

		}

		// follows some examples of a simple programmatic query

		// BooleanQuery.Builder booleanQueryBuilder = new
		// BooleanQuery.Builder();
		// Query vector = new TermQuery(new Term("modelDescription", "vector"));
		// Query space = new TermQuery(new Term("modelDescription", "space"));
		// Query model = new TermQuery(new Term("modelDescription", "model"));

		// booleanQueryBuilder.add(vector, Occur.MUST);
		// booleanQueryBuilder.add(space, Occur.MUST);
		// booleanQueryBuilder.add(model, Occur.MUST);

		BooleanQuery.Builder booleanQueryBuilder = new BooleanQuery.Builder();
		Query vector = new TermQuery(new Term("modelDescription", "vector"));
		Query space = new TermQuery(new Term("modelDescription", "space"));
		Query model = new TermQuery(new Term("modelDescription", "model"));

		booleanQueryBuilder.add(vector, Occur.SHOULD);
		booleanQueryBuilder.add(space, Occur.SHOULD);
		booleanQueryBuilder.add(model, Occur.SHOULD);
		booleanQueryBuilder.setMinimumNumberShouldMatch(1);

		BooleanQuery booleanQuery = booleanQueryBuilder.build();

		// booleanQueryBuilder is a booleanQuery.Builder object
		// the method build(), i.e.
		// booleanQueryBuilder.build() builds a BooleanQuery object
		// BooleanQuery is a subclass of Query

		try {
			topDocs = searcher.search(booleanQuery, 10);
		} catch (IOException e1) {
			System.out.println("Graceful message: exception " + e1);
			e1.printStackTrace();
		}

		System.out.println("\n" + topDocs.totalHits + " results for query \"" + booleanQuery.toString()
				+ "\" showing for the first " + 10
				+ " documents the doc id, score and the content of the modelDescription field");

		for (int i = 0; i < Math.min(10, topDocs.totalHits.value); i++) {
			try {
				System.out.println(topDocs.scoreDocs[i].doc + " -- score: " + topDocs.scoreDocs[i].score + " -- "
						+ reader.document(topDocs.scoreDocs[i].doc).get("modelDescription"));
			} catch (CorruptIndexException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			} catch (IOException e) {
				System.out.println("Graceful message: exception " + e);
				e.printStackTrace();
			}

		}

		try {
			reader.close();
			dir.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}