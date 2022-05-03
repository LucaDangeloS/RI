package udc.rigrado;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
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
        int cut;
        int top;
        String searchmodel = null;
        Similarity similarity;
        float lambda = 0;
        int query1;
        int query2;

        IndexReader reader = null;
        Directory dir = null;
        IndexSearcher searcher = null;
        QueryParser parser;
        Query query = null;


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
                        String querPar = args[++i];
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

        switch (searchmodel.toUpperCase()){
            case "JM":
                similarity = new LMJelinekMercerSimilarity(lambda);
                break;
            case "TFIDF":
                similarity = new ClassicSimilarity();
                break;
            default:
                throw new IllegalArgumentException("Invalid indexingmodel: " + searchmodel);
        }

        searcher = new IndexSearcher(reader);
        searcher.setSimilarity(similarity);
        parser = new QueryParser("contents", new StandardAnalyzer());

    }
}
