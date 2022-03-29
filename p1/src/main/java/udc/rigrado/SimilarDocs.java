package udc.rigrado;

import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import javax.print.Doc;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

public class SimilarDocs implements AutoCloseable {

    private enum RepEnum {
        BIN(),
        TF(),
        TFXIDF()
    }
    private final LinkedHashMap<String, Double> baseTermVector;
    private final int comparingDocID;

    public SimilarDocs(LinkedHashMap<String, Double> baseTermVector, int docID) {
        this.baseTermVector = baseTermVector;
        this.comparingDocID = docID;
    }
    public static double cosineSimilarity(Vector<Double> vec1, Vector<Double> vec2) {
        double dotProd = 0.0;
        double sumSquare1 = 0.0;
        double sumSquare2 = 0.0;
        double v1;
        double v2;
        for (int i = 0; i < vec1.size(); i++) {
            v1 = vec1.get(i);
            v2 = vec2.get(i);
            dotProd += v1 * v2;
            sumSquare1 += Math.pow(v1, 2);
            sumSquare2 += Math.pow(v2, 2);
        }
        return dotProd / (Math.sqrt(sumSquare1 * sumSquare2));
    }

    private static class DocInfo implements Comparable<DocInfo> {
        final int docID;
        final String path;
        double similarity;

        protected DocInfo(int docID, String path, double similarity) {
            this.docID = docID;
            this.path = path;
            this.similarity = similarity;
        }

        @Override
        public int compareTo(DocInfo o) {
            return Double.compare(o.similarity, this.similarity);
        }
    }

    public static void main(String[] args) throws Exception {
        String usage = "java SimilarDocs"
                + " [-index INDEX_PATH] [-docID D] [-field FIELD_NAME]"
                + " [-top N] [-rep bin | tf | tfxidf]\n"
                + "N and D should be positive integers.\n";
        String indexPath = null;
        int top = -1;
        String field = null;
        int docID = -1;
        RepEnum rep = null;

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
                    case "-rep":
                        rep = RepEnum.valueOf(args[++i].toUpperCase());
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
        if (top < 0 || docID < 0 || field == null || indexPath == null || rep == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        IndexReader reader = null;
        try {
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // Obtiene lista de terminos de la colección
        LinkedHashMap<String, Double> baseTerms = getTermsInColl(reader, field);

        try (SimilarDocs sd = new SimilarDocs(baseTerms, docID)) {
            LinkedHashMap<String, Double> map = sd.getTermValuesForDoc(reader, field, rep, sd.comparingDocID);
            LinkedHashMap<String, Double> map2;
            Vector<Double> docVector = new Vector<>(map.values());
            Vector<Double> comparingVector;
            List<DocInfo> similarityList = new ArrayList<>(reader.numDocs()-1);
            DocInfo docInfo = new DocInfo(sd.comparingDocID, reader.document(sd.comparingDocID).get("path"), 1);

            // Itera sobre todos los documentos para obtener la similaridad con todos
            for (int i = 0; i < reader.numDocs(); i++) {
                System.out.print("\rComparing doc " + i + "/" + (reader.numDocs()-1) + "     ");
                if (i != sd.comparingDocID) {
                    map2 = sd.getTermValuesForDoc(reader, field, rep, i);
                    comparingVector = new Vector<>(map2.values());
                    similarityList.add(new DocInfo(i, reader.document(i).get("path"), cosineSimilarity(docVector, comparingVector)));
                }
            }
            System.out.println();
            if (similarityList.size() == 0) {
                System.out.println("There are no other documents");
                System.exit(0);
            }
            // Ordenarla
            Collections.sort(similarityList);

            DocInfo currentDoc;
            String stats = "Doc ID: " + docInfo.docID + "\nPath: " + docInfo.path + "\n";
            stats += "Most similar documents in Collection for field '" + field + "' sorted by " + rep.name() + ":\n";
            System.out.println(stats);

            for (int i = 0; i < top; i++) {
                try {
                    currentDoc = similarityList.get(i);
                    printSimilarity(currentDoc, i);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("\nNo more terms documents available for this collection");
                    break;
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static LinkedHashMap<String, Double> getTermsInColl(IndexReader reader, String fieldName) throws IOException {
        // Se obtiene lista de Leafs de la coleccion
        List<LeafReaderContext> leafList = reader.leaves();
        LinkedHashMap<String, Double> list = new LinkedHashMap<>();

        // Itera sobre los leafs del reader para recoger todos los términos de todos los documentos de la colección
        // Es más eficiente que iterar sobre la lista de terminos de cada documento en la colección
        for (LeafReaderContext leaf : leafList) {
            // Se obtiene terminos del leaf
            Terms terms = leaf.reader().terms(fieldName);
            if (terms != null) {
                TermsEnum te = terms.iterator();
                // Se itera sobre los terminos
                while (te.next() != null) {
                    list.put(te.term().utf8ToString(), 0.0);
                }
            }
        }
        if (list.size() == 0) {
            System.err.println("The field has no term vector");
            System.exit(-1);
        }

        return list;
    }

    private LinkedHashMap<String, Double> getTermValuesForDoc(IndexReader reader, String strField, RepEnum mode, int docID) throws IOException {
        // Obtiene el term vector del documento y el campo
        TermsEnum termVectors = reader.getTermVector(docID, strField).iterator();
        if (termVectors == null) {
            System.err.println("The field specified in the Document has no term vector");
            System.exit(-1);
        }
        PostingsEnum docEnums = null;
        LinkedHashMap<String, Double> values = new LinkedHashMap<>(baseTermVector);
        BytesRef term;
        Double tmp = null;

        while ((term = termVectors.next()) != null) {
            Term tmpterm = new Term(strField, termVectors.term());
            docEnums = termVectors.postings(docEnums, PostingsEnum.FREQS);
            // Avanza una posicion del posting para llegar al documento que se está analizando
            docEnums.nextDoc();
            // Se obtiene document frequency
            // Frecuencia de término para ese documento
            switch (mode) {
                case TF:
                    tmp = (double) docEnums.freq();
                    break;
                case TFXIDF:
                    tmp = docEnums.freq() * Math.log10((double) reader.numDocs() / (double) reader.docFreq(tmpterm));
                    break;
                case BIN:
                    tmp = 1.0;
                    break;
            }
            // Se añade toda la estructura a la lista de frecuencias
            values.put(term.utf8ToString(), tmp);
        }
        return values;
    }

    private static void printSimilarity(DocInfo currentDoc, int i) {
        String stats = "\nNº "  + (i + 1) + ":";
        stats += "\n\tSimilarity: " + currentDoc.similarity;
        stats += "\n\tDoc ID: " + currentDoc.docID;
        stats += "\n\tPath: " + currentDoc.path + "\n";
        System.out.println(stats);
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}
