package simpleindexing;

//Adapted from http://stackoverflow.com/questions/1844194/

//get-cosine-similarity-between-two-documents-in-lucene

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;

public class CosineDocumentSimilarity {

	public static final String CONTENT = "Content";
	private final Set<String> terms = new HashSet<>();
	private final RealVector v1;
	private final RealVector v2;

	CosineDocumentSimilarity(String s1, String s2, String spath) throws IOException {
		Directory directory = createIndex(s1, s2, spath);
		IndexReader reader = DirectoryReader.open(directory);
		Map<String, Integer> f1 = getTermFrequencies(reader, 0);
		Map<String, Integer> f2 = getTermFrequencies(reader, 1);
		reader.close();
		v1 = toRealVector(f1);
		v2 = toRealVector(f2);
	}

	Directory createIndex(String s1, String s2, String spath) throws IOException {

		MMapDirectory directory = new MMapDirectory(Paths.get(spath));

		/*
		 * File-based Directory implementation that uses mmap for reading, and
		 * FSDirectory.FSIndexOutput for writing.
		 * 
		 * RAMDirectory uses inefficient synchronization and is discouraged in lucene
		 * 8.x in favor of MMapDirectory and it will be removed in future versions of
		 * Lucene.
		 */

		Analyzer analyzer = new SimpleAnalyzer();
		IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
		IndexWriter writer = new IndexWriter(directory, iwc);
		addDocument(writer, s1);
		addDocument(writer, s2);
		writer.close();
		return directory;
	}

	/* Indexed, tokenized, stored. */
	public static final FieldType TYPE_STORED = new FieldType();

	static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;

	static {
		TYPE_STORED.setIndexOptions(options);
		TYPE_STORED.setTokenized(true);
		TYPE_STORED.setStored(true);
		TYPE_STORED.setStoreTermVectors(true);
		TYPE_STORED.setStoreTermVectorPositions(true);
		TYPE_STORED.freeze();
	}

	void addDocument(IndexWriter writer, String content) throws IOException {
		Document doc = new Document();
		Field field = new Field(CONTENT, content, TYPE_STORED);
		doc.add(field);
		writer.addDocument(doc);
	}

	double getCosineSimilarity() {
		return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
	}

	public static double getCosineSimilarity(String s1, String s2, String spath) throws IOException {
		return new CosineDocumentSimilarity(s1, s2, spath).getCosineSimilarity();
	}

	Map<String, Integer> getTermFrequencies(IndexReader reader, int docId) throws IOException {
		Terms vector = reader.getTermVector(docId, CONTENT);
		// IndexReader.getTermVector(int docID, String field):
		// Retrieve term vector for this document and field, or null if term
		// vectors were not indexed.
		// The returned Fields instance acts like a single-document inverted
		// index (the docID will be 0).

		// Por esta razon al iterar sobre los terminos la totalTermFreq que es
		// la frecuencia
		// de un termino en la coleccion, en este caso es la frecuencia del
		// termino en docID,
		// es decir, el tf del termino en el documento docID

		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();
		Map<String, Integer> frequencies = new HashMap<>();
		BytesRef text = null;
		while ((text = termsEnum.next()) != null) {
			String term = text.utf8ToString();
			int freq = (int) termsEnum.totalTermFreq();
			frequencies.put(term, freq);
			terms.add(term);
		}
		return frequencies;
	}

	RealVector toRealVector(Map<String, Integer> map) {
		RealVector vector = new ArrayRealVector(terms.size());
		int i = 0;
		for (String term : terms) {
			int value = map.containsKey(term) ? map.get(term) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector.mapDivide(vector.getL1Norm());
		// la división por la norma L1 del vector no es necesaria
		// pero tampoco afecta al calculo del coseno

	}
}
