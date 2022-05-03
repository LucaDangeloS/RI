package udc.rigrado;

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

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Objects;

public class SearchEvalMedline {
    public static void main(String[] args) {
        String usage = "java SearchEvalMedline"
                + " [-indexin INDEX_PATH] [-search jm lambda | tfidf] [-cut n] [-top m] [-queries all | int1 | int1-int2] " + "\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index\n";

        String indexPath = "./index";
        Integer cut = null;
        Integer top = null;
        String searchmodel = null;
        Similarity similarity;
        float lambda = 0;
        Integer query1 = null;
        Integer query2 = null;

        IndexReader reader = null;
        Directory dir = null;
        IndexSearcher searcher = null;
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
                        searchmodel = args[++i];
                        if(Objects.equals(searchmodel.toUpperCase(), "JM")){
                            lambda = Float.parseFloat(args[++i]);
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

        if (searchmodel == null || !(Objects.equals(searchmodel.toUpperCase(), "JM") || Objects.equals(searchmodel.toUpperCase(), "TFIDF"))) {
            System.err.println("-search must be jm lambda or tfidf");
            System.exit(1);
        }

        if(top == null || top < 1){
            System.err.println("-top must be a non negative number");
            System.exit(1);
        }

        if(cut == null || cut < 1){
            System.err.println("-top must be a non negative number");
            System.exit(1);
        }

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            reader = DirectoryReader.open(dir);

        } catch (CorruptIndexException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }

        if(top > reader.numDocs() ){
            System.err.println("-top must be less than the number of documents at index and more than cut");
            System.exit(1);
        }

        if(cut > reader.numDocs()){
            System.err.println("-cut must be less than the number of documents at index");
            System.exit(1);
        }

        switch (searchmodel.toUpperCase()){
            case "JM":
                similarity = new LMJelinekMercerSimilarity(lambda);
                break;
            case "TFIDF":
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


        HashMap<Integer, String> mapQueries = parseQueryDoc(Paths.get("./DocMed/MED.QRY"));
        HashMap<Integer, ArrayList<Integer>> mapRelevance = parseRelevanceDoc(Paths.get("./DocMed/MED.REL"));

        if (Objects.equals(querPar.toUpperCase(), "ALL")){
            query2 = mapQueries.size();
        }

        if(query2 == null){
            query2 = query1;
        }

        if (query2 > mapQueries.size()){
            System.err.println("query range out of bounds");
            System.exit(1);
        }

        int querySize = query2 - query1 + 1;
        ArrayList<String[]> csvMetricValues = new ArrayList<>();
        String[] csvMetricHeaders = {"Query I", "P@" + cut, "Recall@" + cut, "AP@" + cut};
        String[] csvQueryHeaders = new String[querySize + 1];
        for (int i = query1; i <= query2; i++) {
            csvQueryHeaders[i - query1] = String.valueOf(i);
        }

        csvQueryHeaders[csvQueryHeaders.length - 1] = "Promedios";
        StringBuilder fileOutput = new StringBuilder();
        float mean_precition = 0;
        float mean_recall = 0;
        float map = 0;

        for (int i = query1; i <= query2; i++) {

            try {
                query = parser.parse(mapQueries.get(i));
            } catch (ParseException e) {

                e.printStackTrace();
            }

            TopDocs topDocs = null;
            try {
                topDocs = searcher.search(query, Math.max(top, cut));
            } catch (IOException e1) {
                System.out.println("Graceful message: exception " + e1);
                e1.printStackTrace();
            }
            String queryOutput = "\n" + "Results for query: \"" + mapQueries.get(i).trim() + "\" showing for the first " + top + " documents.";
            fileOutput.append(queryOutput + "\n");
            System.out.println(queryOutput);

            for (int j = 0; j < Math.min(top, topDocs.totalHits.value); j++) {
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

            for (int j = 0; j < cut; j++) {
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

            mean_precition += precision;
            mean_recall += recall;

            csvMetricValues.add(new String[]{String.valueOf(precision), String.valueOf(recall), String.valueOf(avgPrecision)});

            String outStr = String.format("P@%d = %.4f  Recall@%d = %.4f AP@%d = %.4f",
                    cut, precision, cut, recall, cut, avgPrecision);
            fileOutput.append(outStr + "\n");
            System.out.println(outStr);

        }
        map = map/(querySize);

        csvMetricValues.add(new String[]{String.valueOf(mean_precition/querySize), String.valueOf(mean_recall/querySize), String.valueOf(map/querySize)});
        createCsv(csvMetricHeaders, csvQueryHeaders, csvMetricValues, outputCsvFile);
        String mapOutput = String.format("\n\nMAP = %.4f", map);
        fileOutput.append(mapOutput + "\n");
        System.out.println(mapOutput);

        createTxt(outputTxtFile, fileOutput.toString());
    }
}
