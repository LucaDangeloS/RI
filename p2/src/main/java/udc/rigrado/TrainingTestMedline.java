package udc.rigrado;

import com.opencsv.CSVWriter;
import org.apache.commons.collections4.queue.TransformedQueue;
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

public class TrainingTestMedline {

    private enum Metrica {P, R, MAP};
    private static String MEDQRY = "./DocMed/MED.QRY";
    private static String MEDREL = "./DocMed/MED.REL";

    public static void main(String[] args) {
        String usage = "java TrainingTestMedline"
                + " [-indexin INDEX_PATH] [-evaltfidf int3-int4 || -evaljm int1-int2 int3-int4] [-cut n] [-metrica P | R | MAP ] " + "\n";

        String indexPath = "./index";
        Integer cut = null;
        String searchmodel = null;
        Similarity similarity;
        String outputCsvFile;
        Metrica metrica = null;
        float lambda = 0;
        Integer trainingQuery1 = null;
        Integer trainingQuery2 = null;
        Integer testQuery1 = null;
        Integer testQuery2 = null;

        IndexReader reader = null;
        Directory dir;
        IndexSearcher searcher;
        QueryParser parser;
        Query query = null;
        String querPar1 = null;
        String querPar2 = null;


        try {
            for (int i = 0; i < args.length; i++) {
                switch (args[i]) {
                    case "-indexin":
                        indexPath = args[++i];
                        break;
                    case "-cut":
                        cut = Integer.parseInt(args[++i]);
                        break;
                    case "-metrica":
                        try {
                            metrica = Metrica.valueOf(args[++i].toUpperCase());
                        } catch (Exception e) {
                            System.out.println("-metrica must be P R or MAP");
                            System.exit(1);
                        }
                        break;
                    case "-evaltfidf":
                        if (searchmodel != null) {
                            System.err.println("Only one search model can be used");
                            System.exit(1);
                        }
                        searchmodel = "TFIDF";
                        querPar1 = args[++i];
                        testQuery1 = Integer.valueOf(querPar1.split("-")[0]);
                        testQuery2 = Integer.valueOf(querPar1.split("-")[0]);
                        break;
                    case "-evaljm":
                        if (searchmodel != null) {
                            System.err.println("Only one search model can be used");
                            System.exit(1);
                        }
                        searchmodel = "JM";
                        querPar1 = args[++i];
                        querPar2 = args[++i];
                        String[] args1 = querPar1.split("-");
                        String[] args2 = querPar2.split("-");
                        trainingQuery1 = Integer.valueOf(args1[0]);
                        trainingQuery2 = Integer.valueOf(args1[1]);
                        testQuery1 = Integer.parseInt(args2[0]);
                        testQuery2 = Integer.parseInt(args2[1]);
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

        if(cut == null || cut < 1){
            System.err.println("-cut must be a non negative number");
            System.exit(1);
        }

        if (metrica == null) {
            System.err.println("-metrica must be P R or MAP");
            System.exit(1);
        }

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            reader = DirectoryReader.open(dir);

        } catch (IOException e1) {
            System.err.println("Exception: " + e1);
            e1.printStackTrace();
        }

        if(cut > reader.numDocs()){
            System.err.println("-cut must be less than the number of documents at index");
            System.exit(1);
        }

        HashMap<Integer, String> mapQueries = parseQueryDoc(Paths.get(MEDQRY));
        HashMap<Integer, ArrayList<Integer>> mapRelevance = parseRelevanceDoc(Paths.get(MEDREL));
        searcher = new IndexSearcher(reader);
        outputCsvFile = String.format("medline.%s.training.%s.test.%s.%s%d.test",
               searchmodel.toLowerCase(), querPar1, querPar2, metrica.toString().toLowerCase(), cut) + ".csv";

        switch (searchmodel) {
            case "JM":
                try {
                    lambda = trainLambda(searcher, trainingQuery1, trainingQuery2, mapQueries, mapRelevance, cut, metrica);
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(1);
                }
                similarity = new LMJelinekMercerSimilarity(lambda);
                break;
            case "TFIDF":
                similarity = new ClassicSimilarity();
                break;
            default:
                throw new IllegalArgumentException("Invalid indexingmodel: " + searchmodel);
        }

        searcher.setSimilarity(similarity);

        parser = new QueryParser("contents", new StandardAnalyzer());

        if ((testQuery2 > mapQueries.size()) || (trainingQuery2 > mapQueries.size())) {
            System.err.println("query range out of bounds");
            System.exit(1);
        }

        int querySize = testQuery2 - testQuery1 + 1;
        ArrayList<String[]> csvMetricValues = new ArrayList<>();
        String[] csvMetricHeaders = {"Query I", "P@" + cut, "Recall@" + cut, "AP@" + cut};
        String[] csvQueryHeaders = new String[querySize + 1];
        for (int i = testQuery1; i <= testQuery2; i++) {
            csvQueryHeaders[i - testQuery1] = String.valueOf(i);
        }

        csvQueryHeaders[csvQueryHeaders.length - 1] = "Promedios";
        StringBuilder fileOutput = new StringBuilder();
        float mean_precision = 0;
        float mean_recall = 0;
        float map = 0;

        for (int i = testQuery1; i <= testQuery2; i++) {

            TopDocs topDocs = null;
            try {
                query = parser.parse(mapQueries.get(i));
                topDocs = searcher.search(query, cut);
            } catch (Exception e) {
                System.err.println("Exception: " + e);
                e.printStackTrace();
            }

            for (int j = 0; j <  topDocs.totalHits.value ; j++) {
                try {
                    String docOutput = searcher.doc(topDocs.scoreDocs[j].doc).get("DocIDMedline") + "\t -- score: " + topDocs.scoreDocs[j].score;
                    fileOutput.append(docOutput + "\n");
                    System.out.println(docOutput);
                } catch (CorruptIndexException e) {
                    System.out.println("Corrupt Index " + e);
                    e.printStackTrace();
                } catch (IOException e) {
                    System.out.println("Exception " + e);
                    e.printStackTrace();
                }

            }

            ArrayList<Integer> relevantDocuments = mapRelevance.get(i);
            float avgPrecision = 0;
            float numerador = 0;
            float precision = 0;
            int corteN = 0;

            for (int j = 0; j < Math.min(cut, topDocs.scoreDocs.length); j++) {
                try {
                    corteN = Integer.parseInt(searcher.doc(topDocs.scoreDocs[j].doc).get("DocIDMedline"));
                } catch (IOException e) {
                    System.out.println("Exception " + e);
                    e.printStackTrace();
                }
                if(relevantDocuments.contains(corteN)){
                    numerador++;
                    precision = numerador/cut;
                    avgPrecision += precision;

                }
            }

            avgPrecision = avgPrecision/relevantDocuments.size();
            map += avgPrecision;
            float recall = numerador/relevantDocuments.size();

            mean_precision += precision;
            mean_recall += recall;

            csvMetricValues.add(new String[]{String.valueOf(precision), String.valueOf(recall), String.valueOf(avgPrecision)});

        }
        map = map/(querySize);

        csvMetricValues.add(new String[]{String.valueOf(mean_precision/querySize), String.valueOf(mean_recall/querySize), String.valueOf(map/querySize)});
        createCsv(csvMetricHeaders, csvQueryHeaders, csvMetricValues, outputCsvFile);

    }

    private static float trainLambda(IndexSearcher searcher, Integer trainingQuery1,
                                     Integer trainingQuery2, HashMap<Integer, String> mapQueries,
                                     HashMap<Integer, ArrayList<Integer>> mapRelevance, Integer cut,
                                     Metrica metrica) throws IOException
    {
        ArrayList<String[]> csvMetricValues = new ArrayList<>();
        ArrayList<Integer> relevantDocuments;
        float numerador = 0;
        float maxMetric = 0;
        int corteN = 0;
        float lambda;

        for (lambda = 0; lambda <= 1; lambda += 0.1) {
            Similarity similarity = new LMJelinekMercerSimilarity(lambda);
            searcher.setSimilarity(similarity);
            TopDocs topDocs = null;

            QueryParser parser = new QueryParser("contents",new StandardAnalyzer());

            for (int i = trainingQuery1; i <= trainingQuery2; i++) {
                relevantDocuments = mapRelevance.get(i);
                try {
                    Query query = parser.parse(mapQueries.get(i));
                    topDocs = searcher.search(query, cut);
                } catch (Exception e) {
                    System.err.println("Exception: " + e);
                    e.printStackTrace();
                }

            }

            for (int j = 0; j < Math.min(cut, topDocs.scoreDocs.length); j++) {
                try {
                    corteN = Integer.parseInt(searcher.doc(topDocs.scoreDocs[j].doc).get("DocIDMedline"));
                } catch (IOException e) {
                    System.out.println("Exception " + e);
                    e.printStackTrace();
                }
                if(relevantDocuments.contains(corteN)){
                    numerador++;
                    precision = numerador/cut;
                    avgPrecision += precision;

                }
            }

        }
        return lambda;
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
                Id_Content = query.split("(\r)*\n.W(\r)*\n");
                Id = Id_Content[0];
                content = Id_Content[1];
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
}