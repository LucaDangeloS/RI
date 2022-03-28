package udc.rigrado;

import org.apache.lucene.index.*;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

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
                + " [-top N] [-order tf | df | idf | tfxidf] [-outputFile FILE]\n"
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

        ArrayList<TermInfo> sortedTerms = getTermInfo(reader, field, order);
        System.out.println("Best terms in Document " + docID + " sorted by " + order.name() + ":");
        String stats;
        for (int i = 0; i < top; i++) {
            try {
                stats = getStats(sortedTerms.get(i));
                System.out.print("\nNº "  + (i+1) + ": " + stats);
            } catch (IndexOutOfBoundsException e) {
                System.out.println("No more terms available for this document");
                break;
            }
        }
    }

    public static class TermInfo implements Comparable<TermInfo> {
        final String term;
        final float df;
        final float tf;
        final float idf;
        final float tfxidf;
        final Order order;

        public TermInfo(String term, float df, float tf, float idf, Order order) {
            this.term = term;
            this.df = df;
            this.tf = tf;
            this.idf = idf;
            this.tfxidf = tf * idf;
            this.order = order;
        }

        @Override
        public int compareTo(TermInfo o) {
            switch (o.order) {
                case DF:
                    return Float.compare(o.df, this.df);
                case TF:
                    return Float.compare(o.tf, this.tf);
                case IDF:
                    return Float.compare(o.idf, this.idf);
                case TFXIDF:
                    return Float.compare(o.tfxidf, this.tfxidf);
                default:
                    throw new IllegalArgumentException("-order option not recognized, must be [df | tf | idf | tfxidf]");
            }
        }
    }

    ArrayList<TermInfo> getTermInfo(IndexReader reader, String strField, Order order) throws IOException {
        TermsEnum termVectors = reader.getTermVector(docID, strField).iterator();
        ArrayList<TermInfo> freqs = new ArrayList<>();
        TFIDFSimilarity similarity = new ClassicSimilarity();
        BytesRef term;

        while ((term = termVectors.next()) != null) {
            Term tmpterm = new Term(strField, termVectors.term());
            float df = reader.docFreq(tmpterm);
            float tf = termVectors.totalTermFreq(); //similarity.tf(termVectors.totalTermFreq());
            float idf = (float) Math.log(reader.numDocs() / df);
            freqs.add(new TermInfo(term.utf8ToString(), df, tf, idf, order));
        }
        Collections.sort(freqs);
        return freqs;
    }

    private String getStats(TermInfo termInfo) {
        return "term '" + termInfo.term + "'"
            + "\t TF= " + termInfo.tf
            + "\t DF= " + termInfo.df
            + "\t IDF= " + termInfo.idf
            + "\t TFxIDF= " + termInfo.tfxidf + "\n";
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}