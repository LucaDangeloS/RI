package udc.rigrado;

import org.apache.lucene.index.*;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import org.apache.lucene.document.Document;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class BestTermsInDoc implements AutoCloseable {

    private enum Order {
        TF(),
        DF(),
        IDF(),
        TFXIDF();

//        private void order(){}
    }

    private final String indexPath;
    private final Order order;
    private final String field;
    private final int docID;
    private final int top;

    private BestTermsInDoc(String indexPath, String field, int docID, int top, Order order) {
        this.indexPath = indexPath;
        this.field = field;
        this.docID = docID;
        this.top = top;
        this.order = order;
    }

    public static void main(String[] args) throws Exception {
        String usage = "java StatsField"
                + " [-index INDEX_PATH] [-docID D] [-field FIELD_NAME]"
                + " [-top N] [-order tf | df | idf | tfxidf] [-outputFile FILE]\n\n"
                + ".\n"
                + "N and D should be positive integers.\n";
        String indexPath = null;
        Order order = null;
        int top = -1;
        String field = null;
        int docID = -1;
        String outputfile = null;

        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-index":
                        indexPath = args[++i];
                        break;
                    case "-docID":
                        docID = Integer.parseInt(args[++i]);
                        break;
                    case "-field":
                        field = args[++i];
                        break;
                    case "-top":
                        top = Integer.parseInt(args[++i]);
                        break;
                    case "-order":
                        order = Order.valueOf(args[++i].toUpperCase());
                        break;
                    case "-outputfile":
                        outputfile = args[++i];
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown parameter " + args[i]);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Usage: " + usage);
            System.err.println(e);
            System.exit(1);
        }

        if (indexPath == null || top < 0 || docID < 0 || order == null || field == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try (BestTermsInDoc statsField = new BestTermsInDoc(indexPath, field, docID, top, order)) {
            statsField.findBestTerms(outputfile);
        }
    }


    private void findBestTerms(String outputfile) throws IOException {
        IndexReader reader = null;
        Terms termVector;

        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        termVector = reader.getTermVector(docID, field);
        if (termVector == null) {
            System.out.println("Document has no term vector");
            System.exit(-1);
        }

        System.out.println(getIdf(reader, field).toString());
//        ArrayList<TermValues> termValuesArray = new ArrayList<>();
//
//
//        while ((tmpTerm = iterator.next()) != null) {
//
//            Term term = new Term(field, tmpTerm);
//            long indexDf = reader.docFreq(term);
//            double idf = classicSimilarity.idf(docsCount, indexDf);
//            int df = reader.docFreq(term);
//            docs = iterator.postings(docs, PostingsEnum.NONE);
//
//            while (docs.nextDoc() != PostingsEnum.NO_MORE_DOCS) {
//
//                double tf = classicSimilarity.tf(docs.freq());
//                termValuesArray.add(new TermValues(term.text(), tf, df, idf));
//            }
//        }
    }

    Map<String, Float> getIdf(IndexReader reader, String strField) throws IOException {
        TermsEnum termVectors = reader.getTermVector(docID, strField).iterator();
        Map<String, Float> freqs = new HashMap<>();
        TFIDFSimilarity similarity = new ClassicSimilarity();
        BytesRef term;

        while ((term = termVectors.next()) != null) {
            Term tmpterm = new Term(strField, termVectors.term());
            float df = reader.docFreq(tmpterm);
            float tf = similarity.tf(termVectors.totalTermFreq());
            float idf = similarity.idf((long) df, reader.numDocs());
            float tfxidf =
                    freqs.put(term.utf8ToString(), idf);
        }
        return freqs;
    }

    private void printStats(CollectionStatistics stats) {
        System.out.println("\nStatistics for field '" + stats.field() + "'");
        System.out.println(
                "\ndocCount= " + stats.docCount()
                        + "\nmaxDoc= " + stats.maxDoc()
                        + "\nsumDocFreq= " + stats.sumDocFreq()
                        + "\nsumTotalFreq= " + stats.sumTotalTermFreq()
        );
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}