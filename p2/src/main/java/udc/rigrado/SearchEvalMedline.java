package udc.rigrado;

import com.opencsv.CSVWriter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;

public class SearchEvalMedline {
    private final static String MEDQRY = "./DocMed/MED.QRY";
    private final static String MEDREL = "./DocMed/MED.REL";

    private enum SearchModel {
        JM,
        TFIDF
    }

    // Data type to hold precision, recall and MAP
    private static class Metrics {
        public float precision = 0;
        public float AP = 0;
        public float recall = 0;
        public float map = 0;
        public boolean valid;

        public Metrics(float precision, float AP, float recall, float map) {
            this.precision = precision;
            this.AP = AP;
            this.recall = recall;
            this.map = map;
            this.valid = true;
        }
        public Metrics() {
            this.valid = false;
        }
    }

    public static void main(String[] args) {
        String usage = "java SearchEvalMedline"
                + " [-indexin INDEX_PATH] [-search jm lambda | tfidf] [-cut n] [-top m] [-queries all | int1 | int1-int2] " + "\n\n"
                + "This shows top m documents of each query and metrics such as precission recall and average precission at cut n.\n";

        String indexPath = "./index";
        Integer cut = null;
        Integer top = null;
        SearchModel searchmodel = null;
        Similarity similarity;
        String outputCsvFile;
        String outputTxtFile;
        float lambda = 0;
        Integer query1 = null;
        Integer query2 = null;

        IndexReader reader = null;
        Directory dir;
        IndexSearcher searcher;
        QueryParser parser;
        Query query = null;
        String querPar = null;

        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-indexin":
                        indexPath = args[++i];
                        break;
                    case "-search":
                        try {
                            searchmodel = SearchModel.valueOf(args[++i].toUpperCase());
                            if (searchmodel == SearchModel.JM) {
                                lambda = Float.parseFloat(args[++i]);
                            }
                        } catch (Exception e) {
                            System.err.println("-search must be jm lambda or tfidf");
                            System.exit(1);
                        }
                        break;
                    case "-cut":
                        cut = Integer.parseInt(args[++i]);
                        break;
                    case "-top":
                        top = Integer.parseInt(args[++i]);
                        break;
                    case "-queries":
                        querPar = args[++i];
                        if(querPar.contains("-")){
                            String[] rang = querPar.split("-");
                            query1 = Integer.parseInt(rang[0]);
                            query2 = Integer.parseInt(rang[1]);
                        } else if (Objects.equals(querPar.toUpperCase(), "ALL")) {
                            query1 = 0;
                        } else {
                            query1 = Integer.parseInt(querPar);
                        }

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

        if (searchmodel == null || top == null || cut == null || indexPath == null || querPar == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if(top < 1){
            System.err.println("-top must be a non negative number");
            System.exit(1);
        }

        if(cut < 1){
            System.err.println("-cut must be a non negative number");
            System.exit(1);
        }

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            reader = DirectoryReader.open(dir);

        } catch (IOException e) {
            System.err.println("Exception: " + e);
            e.printStackTrace();
        }

        if (reader == null) {
            System.err.println("Index not found");
            System.exit(1);
        }

        if(top > reader.numDocs() ){
            System.err.println("-top must be less than the number of documents at index and more than cut");
            System.exit(1);
        }

        if(cut > reader.numDocs()){
            System.err.println("-cut must be less than the number of documents at index");
            System.exit(1);
        }

        switch (searchmodel){
            case JM:
                similarity = new LMJelinekMercerSimilarity(lambda);
                String strlambda = Float.toString(lambda);
                outputCsvFile = String.format("medline.jm.%d.cut.lambda.%s.q%s.csv", cut, strlambda, querPar);
                outputTxtFile = String.format("medline.jm.%d.hits.lambda.%s.q%s.txt", top, strlambda, querPar);
                break;
            case TFIDF:
                similarity = new ClassicSimilarity();
                outputCsvFile = "medline.tfidf." + cut + ".cut.q" + querPar + ".csv";
                outputTxtFile = "medline.tfidf." + top + ".hits.q" + querPar + ".txt";
                break;
            default:
                throw new IllegalArgumentException("Invalid indexingmodel: " + searchmodel);
        }

        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(similarity);

        parser = new QueryParser("contents", new StandardAnalyzer());

        HashMap<Integer, String> mapQueries = parseQueryDoc(Paths.get(MEDQRY));
        HashMap<Integer, ArrayList<Integer>> mapRelevance = parseRelevanceDoc(Paths.get(MEDREL));

        if (Objects.equals(querPar.toUpperCase(), "ALL")){
            query2 = mapQueries.size();
        }

        if(query2 == null){
            query2 = query1;
        }

        if (query2 > mapQueries.size()){
            System.err.println("Query range out of bounds");
            System.exit(1);
        }

        int querySize = query2 - query1 + 1;
        ArrayList<String[]> csvMetricValues = new ArrayList<>();
        String[] csvMetricHeaders = {"Query I", "P@" + cut, "R@" + cut, "AP@" + cut};
        String[] csvQueryHeaders = new String[querySize + 1];
        for (int i = query1; i <= query2; i++) {
            csvQueryHeaders[i - query1] = String.valueOf(i);
        }

        csvQueryHeaders[csvQueryHeaders.length - 1] = "Promedios";
        StringBuilder fileOutput = new StringBuilder();
        float mean_precision = 0;
        float mean_recall = 0;
        float map = 0;
        Metrics metrics = new Metrics();

        for (int i = query1; i <= query2; i++) {

            TopDocs topDocs = null;
            // Obtiene resultados de query y imprime la query
            try {
                query = parser.parse(mapQueries.get(i));
                topDocs = null;
                topDocs = searcher.search(query, Math.max(top, cut));
                String queryOutput = "\n" + "Results for query: \"" + mapQueries.get(i).trim() + "\" showing for the first " + top + " documents.";
                fileOutput.append(queryOutput + "\n");
                System.out.println(queryOutput);
            } catch (Exception e) {
                System.err.println("Exception: " + e);
                e.printStackTrace();
            }

            // Imprime los scores y obtiene las métricas
            for (int j = 0; j < Math.min(top, topDocs.totalHits.value); j++) {
                try {
                    String docOutput = searcher.doc(topDocs.scoreDocs[j].doc).get("DocIDMedline") + "\t -- score: " + topDocs.scoreDocs[j].score;
                    fileOutput.append(docOutput + "\n");
                    System.out.println(docOutput);

                    ArrayList<Integer> relevantDocuments = mapRelevance.get(i);
                    metrics = getMetricsFromQuery(query, searcher, relevantDocuments, cut, querySize);

                } catch (CorruptIndexException e) {
                    System.err.println("Corrupt Index " + e);
                    e.printStackTrace();
                } catch (IOException e) {
                    System.err.println("Exception: " + e);
                    e.printStackTrace();
                }
            }

            if (!metrics.valid) querySize--;
            if (querySize == 0) break;

            mean_precision += metrics.precision;
            mean_recall += metrics.recall;
            map += metrics.AP;

            csvMetricValues.add(new String[]{String.valueOf(metrics.precision), String.valueOf(metrics.recall), String.valueOf(metrics.AP)});

            String outStr = String.format("P@%d = %.4f  Recall@%d = %.4f AP@%d = %.4f",
                    cut, metrics.precision, cut, metrics.recall, cut, metrics.AP);
            fileOutput.append(outStr + "\n");
            System.out.println(outStr);
        }

        map = map / querySize;
        mean_precision = mean_precision/(querySize);
        mean_recall = mean_recall/(querySize);

        csvMetricValues.add(new String[]{String.valueOf(mean_precision), String.valueOf(mean_recall), String.valueOf(map)});
        createCsv(csvMetricHeaders, csvQueryHeaders, csvMetricValues, outputCsvFile);

        String mapOutput = String.format("\n\nMean_Precision = %.4f  Mean_Recall = %.4f  MAP = %.4f", mean_precision, mean_recall, map);
        fileOutput.append(mapOutput + "\n");
        System.out.println(mapOutput);

        createTxt(outputTxtFile, fileOutput.toString());
    }

    private static Metrics getMetricsFromQuery(Query query, IndexSearcher searcher, ArrayList<Integer> relevantDocuments, int cut, int qSize) throws IOException {
        float avgPrecision = 0;
        int querySize = qSize;
        float numerador = 0;
        float precision;
        int docN;

        TopDocs topDocs = searcher.search(query, cut);
        if (topDocs == null) return new Metrics();

        for (int j = 0; j < Math.min(cut, topDocs.totalHits.value); j++) {
            docN = Integer.parseInt(searcher.doc(topDocs.scoreDocs[j].doc).get("DocIDMedline"));

            if(relevantDocuments.contains(docN)) {
                numerador++;
                precision = numerador/(j+1); // (j+1) o cut?
                avgPrecision += precision;
            }
        }

        avgPrecision = avgPrecision/relevantDocuments.size();
        if (topDocs.totalHits.value == 0) querySize--;
        if (querySize == 0) return new Metrics();

        return new Metrics(numerador/cut, avgPrecision, numerador/relevantDocuments.size(), avgPrecision/querySize);
    }

    private static HashMap<Integer, String> parseQueryDoc(Path path) {
        HashMap<Integer, String> queries = new HashMap<>();
        
        try (InputStream stream = Files.newInputStream(path)) {
            String str = new String(stream.readAllBytes());
            String[] docsMED = str.split(".I ");
            String query;
            String[] Id_Content;
            String content;
            String Id;

            for (int i = 1; i < docsMED.length; i++) {
                query = docsMED[i];
                Id_Content = query.split("(\r)?\n.W(\r)?\n");
                Id = Id_Content[0];
                content = Id_Content[1].toLowerCase().replaceAll("\\(|\\)", "");
                queries.put(Integer.parseInt(Id), content);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return queries;
    }

    private static HashMap<Integer, ArrayList<Integer>> parseRelevanceDoc(Path path) {
        HashMap<Integer, ArrayList<Integer>> relevance = new HashMap<>();

        try (InputStream stream = Files.newInputStream(path)) {
            String str = new String(stream.readAllBytes());
            ArrayList<String> lines = new ArrayList<>();
            str.lines().forEach(lines::add);

            ArrayList<Integer> docRelevance = new ArrayList<>();
            String[] splitLine;
            // First document ID in MED.QRY
            int lastId = Integer.parseInt(lines.get(0).split(" ")[0]);
            int currID;

            for (String line: lines) {
                splitLine = line.split(" ");
                currID = Integer.parseInt(splitLine[0]);

                if (currID != lastId) {
                    relevance.put(lastId, docRelevance);
                    docRelevance = new ArrayList<>();
                }
                
                lastId = currID;
                docRelevance.add(Integer.valueOf(splitLine[2]));
            }
            relevance.put(lastId, docRelevance);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return relevance;
    }

    private static void createCsv(String[] header, String[] queries, ArrayList<String[]> metrics, String outputPath) {
        try {
            FileWriter outputfile = new FileWriter(outputPath);
            CSVWriter writer = new CSVWriter(outputfile);
            writer.writeNext(header);
            String[] line;
            int i = 0;

            for (String[] row: metrics) {
                line = new String[row.length + 1];
                line[0] = queries[i];
                System.arraycopy(row, 0, line, 1, row.length);
                writer.writeNext(line);
                i++;
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createTxt(String outputPath, String output) {
        try{
            FileWriter outputfile = new FileWriter(outputPath);
            outputfile.write(output);

            outputfile.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

//Terminado el 4/5/2022f


}
