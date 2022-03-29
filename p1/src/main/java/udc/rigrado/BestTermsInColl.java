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
            System.err.println("Top should be a positive integer");
            System.exit(1);
        }

        if (field == null) {
            System.err.println("A field must be specified");
            System.exit(1);
        }

        if (indexPath == null) {
            System.err.println("Index path should be specified");
            System.exit(1);
        }


        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        // Se obtiene la lista de termino de la colección ya ordenada
        ArrayList<TermInfo> topTerms = getTopTerms(reader, field, rev);
        String stats = "Best terms in Collection for field '" + field + "' sorted by ";
        if (rev)
            stats += "DF:";
        else
            stats += "IDFLOG10:";

        System.out.println(stats);
        TermInfo ti = null;
        // Se itera sobre los top N terminos de la lista de terminos
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
                return Integer.compare(o.df, this.df);
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
        // Se obtiene lista de Leafs de la coleccion
        List<LeafReaderContext> leafList = reader.leaves();
        HashMap<String, Integer> map = new HashMap<>();
        Integer tmpDf = null;

        // Itera sobre los leafs del reader para recoger todos los términos de todos los documentos de la colección
        // Es más eficiente que iterar sobre la lista de terminos de cada documento en la colección
        for (LeafReaderContext leaf : leafList) {
            // Se obtiene terminos del leaf
            Terms terms = leaf.reader().terms(fieldName);
            if (terms != null) {
                TermsEnum te = terms.iterator();
                // Se itera sobre los terminos
                while (te.next() != null) {
                    if ((tmpDf = map.get(te.term().utf8ToString())) == null) {
                        // Si el valor no existía en el hashmap, se añade
                        map.put(te.term().utf8ToString(), te.docFreq());
                    } else {
                        // Si el valor existía previamente, se suman las frecuencias
                        map.put(te.term().utf8ToString(), tmpDf + te.docFreq());
                    }
                }
            }
        }
        if (map.size() == 0) {
            System.err.println("The field has no term vector");
            System.exit(-1);
        }
        return map;
    }

    private static ArrayList<TermInfo> getTopTerms(IndexReader reader, String strField, boolean rev) throws IOException {
        ArrayList<TermInfo> freqs = new ArrayList<>();
        HashMap<String, Integer> map = getDfOfTermsInColl(reader, strField);


        // Añade cada elemento de hashmap a la lista para que se pueda ordenar
        if (rev) {
            // Si se pide rev, solo se añade la frecuencia de documento del termino
            map.forEach((k, v) -> freqs.add(new TermInfo(k, v)));
        } else {
            // Si no, se calcula el idflog10 del termino
            double totalDocs = reader.numDocs();
            map.forEach((k, v) -> freqs.add(new TermInfo(k, Math.log10(totalDocs / (double) v))));
        }
        // Ordena la lista de frecuencias
        Collections.sort(freqs);
        return freqs;
    }
}
