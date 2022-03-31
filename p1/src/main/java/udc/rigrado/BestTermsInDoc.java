package udc.rigrado;

import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;

public class BestTermsInDoc implements AutoCloseable {

    private enum Order {
        TF(),
        DF(),
        IDF(),
        TFXIDF()
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
        String usage = "java BestTermInDoc"
                + " [-index INDEX_PATH] [-docID D] [-field FIELD_NAME]"
                + " [-top N] [-order tf | df | idf | tfxidf] [-outputfile FILE]\n"
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
            e.printStackTrace();
            System.exit(1);
        }

        if (indexPath == null) {
            System.err.println("An index path must be given");
            System.exit(1);
        }

        if (field == null) {
            System.err.println("A field must be specified");
            System.exit(1);
        }

        if (top < 0 || docID < 0 || order == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }
        // Se instancia la clase y se llama el método
        try (BestTermsInDoc statsField = new BestTermsInDoc(indexPath, field, docID, top, order)) {
            statsField.findBestTerms(outputfile);
        }
    }


    private void findBestTerms(String outputfile) throws IOException {
        IndexReader reader = null;
        Terms termVector;
        FileWriter outputFile = null;

        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Genera la lista de términos ya ordenada
        ArrayList<TermInfo> sortedTerms = getTermInfo(reader, field, order);

        // Crea la variable en  la que se va a guardar cada línea de print o escritura
        String stats = "Best terms in Document " + docID + " for field '" + field + "' sorted by " + order.name() + ":";

        // Crea el output file pasado como argumento
        if (outputfile != null) {
            outputFile = new FileWriter(outputfile);
            try { outputFile.write(stats); }
            catch (IOException e) { throw new IOException("Error writing to output file provided"); }
        }
        else {
            System.out.println(stats);
        }

        // Se itera sobre los top N elementos de la lista de términos
        for (int i = 0; i < top; i++) {
            try {
                stats = "\nNº "  + (i + 1) + ": " + getStats(sortedTerms.get(i));
                if (outputfile == null)
                    System.out.print(stats);
                else {
                    outputFile.write(stats);
                }
            } catch (IndexOutOfBoundsException e) {
                System.out.println("\nNo more terms available for this document");
                break;
            }
        }
        if (outputFile != null) {
            outputFile.flush();
            outputFile.close();
        }
    }

    private static class TermInfo implements Comparable<TermInfo> {
        final String term;
        final float df;
        final float tf;
        final double idf;
        final double tfxidf;
        final Order order;

        public TermInfo(String term, float df, float tf, double idf, Order order) {
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
                    return Double.compare(o.idf, this.idf);
                case TFXIDF:
                    return Double.compare(o.tfxidf, this.tfxidf);
                default:
                    throw new IllegalArgumentException("-order option not recognized, must be [df | tf | idf | tfxidf]");
            }
        }
    }

    private ArrayList<TermInfo> getTermInfo(IndexReader reader, String strField, Order order) throws IOException {
        // Obtiene el term vector del documento y el campo
        TermsEnum termVectors = reader.getTermVector(docID, strField).iterator();
        if (termVectors == null) {
            System.err.println("The field specified in the Document has no term vector");
            System.exit(-1);
        }
        PostingsEnum docEnums = null;
        ArrayList<TermInfo> freqs = new ArrayList<>();
        BytesRef term;

        // Itera sobre el term vector, calculando y almacenando df, tf e idf para cada uno
        while ((term = termVectors.next()) != null) {
            Term tmpterm = new Term(strField, termVectors.term());
            docEnums = termVectors.postings(docEnums, PostingsEnum.FREQS);
            // Avanza una posicion del posting para llegar al documento que se está analizando
            docEnums.nextDoc();
            // Se obtiene document frequency
            float df = reader.docFreq(tmpterm);
            // Frecuencia de término para ese documento
            float tf = docEnums.freq();
            // idflog10 (inversa de frecuencia de documento log 10)
            double idf = Math.log10((double) reader.numDocs() / (double) df);
            // Se añade toda la estructura a la lista de frecuencias
            freqs.add(new TermInfo(term.utf8ToString(), df, tf, idf, order));
        }
        // Se ordena la lista
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