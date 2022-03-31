package udc.rigrado;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.*;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

import javax.print.Doc;

public class DocClusters implements AutoCloseable{

    private enum RepEnum {
        BIN(),
        TF(),
        TFXIDF()
    }

    private final LinkedHashMap<String, Double> baseTermVector;
    private final int comparingDocID;

    public DocClusters(LinkedHashMap<String, Double> baseTermVector, int docID) {
        this.baseTermVector = baseTermVector;
        this.comparingDocID = docID;
    }

    private static class DocInfo implements Comparable<DocClusters.DocInfo> {
        final int docID;
        final String path;
        double similarity;

        protected DocInfo(int docID, String path, double similarity) {
            this.docID = docID;
            this.path = path;
            this.similarity = similarity;
        }

        @Override
        public int compareTo(DocClusters.DocInfo o) {
            return Double.compare(o.similarity, this.similarity);
        }
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

    public static void main(String[] args) throws Exception{
        String usage = "java DocClusters"
                + " [-index INDEX_PATH] [-field FIELD_NAME] [-doc D] "
                + " [-top N] [-rep bin | tf | tfxidf] [-k NUM_CLUSTERS]\n"
                + "N and D should be positive integers.\n";

        String indexPath = null;
        int top = -1;
        String field = null;
        int docID = -1;
        DocClusters.RepEnum rep = null;
        int k = -1;


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
                        rep = DocClusters.RepEnum.valueOf(args[++i].toUpperCase());
                        break;
                    case "-k":
                        k = Integer.parseInt(args[++i]);
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

        if (k < 0 || top < 0 || docID < 0 || field == null || indexPath == null || rep == null) {
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

        LinkedHashMap<String, Double> baseTerms = getTermsInColl(reader, field);

        try (DocClusters dc = new DocClusters(baseTerms, docID)) {
            LinkedHashMap<String, Double> map = dc.getTermValuesForDoc(reader, field, rep, dc.comparingDocID);
            LinkedHashMap<String, Double> map2;
            Vector<Double> docVector = new Vector<>(map.values());
            Vector<Double> comparingVector;
            List<DocClusters.DocInfo> similarityList = new ArrayList<>(reader.numDocs()-1);
            DocClusters.DocInfo docInfo = new DocInfo(dc.comparingDocID, reader.document(dc.comparingDocID).get("path"), 1);

            List<Punto> puntos = new ArrayList<>(reader.numDocs()-1);
            Double[] data = docVector.toArray(new Double[0]);
            Punto punto1 = new Punto(data, String.valueOf(dc.comparingDocID));
            Double[] dataN;
            Punto puntoN;
            puntos.add(punto1);
            // Itera sobre todos los documentos para obtener la similaridad con todos
            for (int i = 0; i < reader.numDocs(); i++) {
                System.out.print("\rComparing doc " + i + "/" + (reader.numDocs()-1) + "     ");
                if (i != dc.comparingDocID) {
                    map2 = dc.getTermValuesForDoc(reader, field, rep, i);
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

            DocClusters.DocInfo currentDoc;
            String stats = "Doc ID: " + docInfo.docID + "\nPath: " + docInfo.path + "\n";
            stats += "Most similar documents in Collection for field '" + field + "' sorted by " + rep.name() + ":\n";
            System.out.println(stats);
            DecimalFormat df = new DecimalFormat();
            df.setMaximumFractionDigits(2);

            for (int i = 0; i < top; i++) {
                try {
                    currentDoc = similarityList.get(i);
                    puntoN = new Punto(dc.getTermValuesForDoc(reader, field, rep, currentDoc.docID).values().toArray(new Double[0]), String.valueOf(currentDoc.docID));
                    puntos.add(puntoN);
                    printSimilarity(currentDoc, i, df);
                } catch (IndexOutOfBoundsException e) {
                    System.out.println("\nNo more documents available for this collection");
                    break;
                }
            }

            KMeans kMeans = new KMeans();
            KMeansResultado resultado = kMeans.calcular(puntos, k);
            List<Cluster> listClusters = resultado.getClusters();
            for(int i = 0; i < listClusters.size();i++){
                System.out.println("\nCluster " + (i+1) + ": ");
                System.out.println(listClusters.get(i).toString());
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

    private LinkedHashMap<String, Double> getTermValuesForDoc(IndexReader reader, String strField, DocClusters.RepEnum mode, int docID) throws IOException {
        // Obtiene el term vector del documento y el campo
        TermsEnum termVectors = reader.getTermVector(docID, strField).iterator();           //Si devuelve nulo no va a la siguiente linea, salta un nullpointer
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

    private static void printSimilarity(DocClusters.DocInfo currentDoc, int i, DecimalFormat df) {
        String stats = "\nNº "  + (i + 1) + ":";
        stats += "\n\tSimilarity: " + df.format(currentDoc.similarity*100)+"%";
        stats += "\n\tDoc ID: " + currentDoc.docID;
        stats += "\n\tPath: " + currentDoc.path + "\n";
        System.out.println(stats);
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}

//Estas clases de abajo pertenecen a 

class Punto {
    private Double[] data;
    String name;

    public Punto(String[] strings) {
        super();
        List<Double> puntos = new ArrayList<Double>();
        for (String string : strings) {
            puntos.add(Double.parseDouble(string));
        }
        this.data = puntos.toArray(new Double[strings.length]);
    }

    public Punto(Double[] data) {
        this.data = data;
    }

    public Punto(Double[] data, String name) {
        this.data = data; this.name =name;
    }

    public double get(int dimension) {
        return data[dimension];
    }

    public int getGrado() {
        return data.length;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(data[0]);
        for (int i = 1; i < data.length; i++) {
            sb.append(", ");
            sb.append(data[i]);
        }
        return sb.toString();
    }

    public Double distanciaEuclideana(Punto destino) {
        Double d = 0d;
        for (int i = 0; i < data.length; i++) {
            d += Math.pow(data[i] - destino.get(i), 2);
        }
        return Math.sqrt(d);
    }

    @Override
    public boolean equals(Object obj) {
        Punto other = (Punto) obj;
        for (int i = 0; i < data.length; i++) {
            if (data[i] != other.get(i)) {
                return false;
            }
        }
        return true;
    }
}
//
 class Cluster {
    private List<Punto> puntos = new ArrayList<Punto>();
    private Punto centroide;
    private boolean termino = false;

    public Punto getCentroide() {
        return centroide;
    }

    public void setCentroide(Punto centroide) {
        this.centroide = centroide;
    }

    public List<Punto> getPuntos() {
        return puntos;
    }

    public boolean isTermino() {
        return termino;
    }

    public void setTermino(boolean termino) {
        this.termino = termino;
    }

    public void limpiarPuntos() {
        puntos.clear();
    }

    @Override
    public String toString() {
        StringBuilder result = new StringBuilder();
        for (Punto punto : puntos) {
            result.append("\tDoc ID:" +punto.name + "\n");
        }

        return result.toString();
    }
}

class KMeansResultado {
    private List<Cluster> clusters = new ArrayList<Cluster>();
    private Double ofv;

    public KMeansResultado(List<Cluster> clusters, Double ofv) {
        super();
        this.ofv = ofv;
        this.clusters = clusters;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public Double getOfv() {
        return ofv;
    }
}

class KMeans {
    public KMeansResultado calcular(List<Punto> puntos, Integer k) {
        List<Cluster> clusters = elegirCentroides(puntos, k);

        while (!finalizo(clusters)) {
            prepararClusters(clusters);
            asignarPuntos(puntos, clusters);
            recalcularCentroides(clusters);
        }

        Double ofv = calcularFuncionObjetivo(clusters);

        return new KMeansResultado(clusters, ofv);
    }

    private void recalcularCentroides(List<Cluster> clusters) {
        for (Cluster c : clusters) {
            if (c.getPuntos().isEmpty()) {
                c.setTermino(true);
                continue;
            }

            Double[] d = new Double[c.getPuntos().get(0).getGrado()];
            Arrays.fill(d, (double)0f);
            for (Punto p : c.getPuntos()) {
                for (int i = 0; i < p.getGrado(); i++) {
                    d[i] += (p.get(i) / c.getPuntos().size());
                }
            }

            Punto nuevoCentroide = new Punto(d);

            if (nuevoCentroide.equals(c.getCentroide())) {
                c.setTermino(true);
            } else {
                c.setCentroide(nuevoCentroide);
            }
        }
    }

    private void asignarPuntos(List<Punto> puntos, List<Cluster> clusters) {
        for (Punto punto : puntos) {
            Cluster masCercano = clusters.get(0);
            Double distanciaMinima = Double.MAX_VALUE;
            for (Cluster cluster : clusters) {
                Double distancia = punto.distanciaEuclideana(cluster
                        .getCentroide());
                if (distanciaMinima > distancia) {
                    distanciaMinima = distancia;
                    masCercano = cluster;
                }
            }
            masCercano.getPuntos().add(punto);
        }
    }

    private void prepararClusters(List<Cluster> clusters) {
        for (Cluster c : clusters) {
            c.limpiarPuntos();
        }
    }

    private Double calcularFuncionObjetivo(List<Cluster> clusters) {
        Double ofv = 0d;

        for (Cluster cluster : clusters) {
            for (Punto punto : cluster.getPuntos()) {
                ofv += punto.distanciaEuclideana(cluster.getCentroide());
            }
        }

        return ofv;
    }

    private boolean finalizo(List<Cluster> clusters) {
        for (Cluster cluster : clusters) {
            if (!cluster.isTermino()) {
                return false;
            }
        }
        return true;
    }

    private List<Cluster> elegirCentroides(List<Punto> puntos, Integer k) {
        List<Cluster> centroides = new ArrayList<Cluster>();

        List<Double> maximos = new ArrayList<Double>();
        List<Double> minimos = new ArrayList<Double>();
        // me fijo máximo y mínimo de cada dimensión

        for (int i = 0; i < puntos.get(0).getGrado(); i++) {
            Double min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY;

            for (Punto punto : puntos) {
                min = min > punto.get(i) ? punto.get(i) : min;
                max = max < punto.get(i) ? punto.get(i) : max;
            }

            maximos.add(max);
            minimos.add(min);
        }

        Random random = new Random();

        for (int i = 0; i < k; i++) {
            Double[] data = new Double[puntos.get(0).getGrado()];
            Arrays.fill(data, (double)0f);
            for (int d = 0; d < puntos.get(0).getGrado(); d++) {
                data[d] = random.nextFloat()
                        * (maximos.get(d) - minimos.get(d)) + minimos.get(d);
            }

            Cluster c = new Cluster();
            Punto centroide = new Punto(data);
            c.setCentroide(centroide);
            centroides.add(c);
        }

        return centroides;
    }
}