package udc.rigrado;

import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class BestTermsInColl {

    public static void main(String[] args) throws IOException {
        String usage = "java BestTermsInColl"
                + " [-index INDEX_PATH] [-field FIELD_NAME]"
                + " [-top N] [-rev]\n";
        String indexPath = null;
        String field = null;
        boolean rev = false;
        int top = -1;

        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-index":
                        indexPath = args[++i];
                        break;
                    case "-field":
                        field = args[++i];
                        break;
                    case "-top":
                        top = Integer.parseInt(args[++i]);
                        break;
                    case "-rev":
                        rev = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown parameter " + args[i]);
                }
            }
        } catch (IllegalArgumentException e) {
            System.err.println("Usage: " + usage);
            e.printStackTrace();
            System.exit(1);
        }

        if (top < 0) {
            System.err.println("Top should be a positive integer.");
            System.exit(1);
        }

        if (indexPath == null) {
            System.err.println("Index path should be specified.");
            System.exit(1);
        }

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        ArrayList<TermInfo> topTerms = getTopTerms(reader, field, rev);
        String stats = "Best terms in Collection for field '" + field + "' sorted by ";
        if (rev)
            stats += "DF:";
        else
            stats += "IDFLOG10:";

        System.out.println(stats);
        TermInfo ti = null;

        for (int i = 0; i < top; i++) {
            try {
                ti = topTerms.get(i);
                stats = "\nNº "  + (i + 1) + ":\n\t";
                stats += ti.term + " (" + ti.getValue() + ")";
                System.out.print(stats);
            } catch (IndexOutOfBoundsException e) {
                System.out.println("\nNo more terms available for this collection");
                break;
            }
        }
    }

    private static class TermInfo implements Comparable<TermInfo> {
        final String term;
        Double idf = null;
        Integer df = null;

        public TermInfo(String term, double idf) {
            this.term = term;
            this.idf = idf;
        }
        public TermInfo(String term, int df) {
            this.term = term;
            this.df = df;
        }

        @Override
        public int compareTo(TermInfo o) {
            if (this.df == null)
                return Double.compare(o.idf, this.idf);
            else
                return Float.compare(o.df, this.df);
        }

        public Number getValue() {
            if (this.df == null)
                return this.idf;
            else
                return this.df;
        }

        @Override
        public boolean equals(Object obj) {
            if (this.getClass() != obj.getClass())
                return false;
            TermInfo o = (TermInfo) obj;
            if (this.getValue() == null || o.getValue() == null)
                return false;
            else
                return Objects.equals(this.getValue(), o.getValue());
        }
    }

    private static HashMap<String, Integer> getDfOfTermsInColl(IndexReader reader, String fieldName) throws IOException {
        // Itera sobre los leafs del reader para recoger todos los términos de todos los documentos de la colección
        List<LeafReaderContext> leafList = reader.leaves();
        HashMap<String, Integer> map = new HashMap<>();
        Integer tmpDf = null;

        for (LeafReaderContext leaf : leafList) {
            Terms terms = leaf.reader().terms(fieldName);
            if (terms == null) {
                System.err.println("The field has no term vector");
                System.exit(-1);
            }
            TermsEnum te = terms.iterator();
            while (te.next() != null) {
                if ((tmpDf = map.get(te.term().utf8ToString())) == null) {
                    // If value had not been found before, introduce it
                    map.put(te.term().utf8ToString(), te.docFreq());
                } else {
                    // If value had been found before, update its value adding to the total
                    map.put(te.term().utf8ToString(), tmpDf + te.docFreq());
                }
            }
        }
        return map;
    }

    private static ArrayList<TermInfo> getTopTerms(IndexReader reader, String strField, boolean rev) throws IOException {
        ArrayList<TermInfo> freqs = new ArrayList<>();
        HashMap<String, Integer> map = getDfOfTermsInColl(reader, strField);

        // Add each entry of the hashmap to the list of terms to then later sort them
        if (rev) {
            map.forEach((k, v) -> freqs.add(new TermInfo(k, v)));    // If rev is required, just add the total docFreq for each term
        } else {
            // If not, calculate de idflog10 for each term
            double totalDocs = reader.numDocs();
            map.forEach((k, v) -> freqs.add(new TermInfo(k, Math.log10(totalDocs / (double) v))));
        }
        // Sort the frequencies list
        Collections.sort(freqs);
        return freqs;
    }
}
