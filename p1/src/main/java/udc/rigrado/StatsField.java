package udc.rigrado;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;

public class StatsField implements AutoCloseable {

    public static void main(String[] args) throws Exception {
        String usage = "java StatsField"
                + " [-index INDEX_PATH] [-field FIELD_NAME]\n\n"
                + "This shows statistics for a field in an index.\n"
                + "If no field is specified, it will list the stats for all the fields in the index.\n";
        String indexPath = null;
        String field = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-field":
                    field = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter " + args[i]);
            }
        }

        if (indexPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try (StatsField statsField = new StatsField()) {
            statsField.readStats(indexPath, field);
        }
    }


    private void readStats(String indexPath, String field) {
        CollectionStatistics collectionStats = null;
        HashSet<IndexableField> fieldList = new HashSet<>();

        try {
            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            DirectoryReader indexReader = DirectoryReader.open(indexDir);
            IndexSearcher searcher = new IndexSearcher(indexReader);

            if (field != null)
                printStats(searcher.collectionStatistics(field));
            else {
                for (int i = 0; i < indexReader.numDocs(); i++) {
                    fieldList.addAll(indexReader.document(i).getFields());
                }
                for (IndexableField idField: fieldList) {
                    printStats(searcher.collectionStatistics(idField.name()));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void printStats(CollectionStatistics stats) {
        System.out.println("\nStatistics for field '" + stats.field() + "'");
        System.out.println(
              "\ndocCount= " + stats.docCount()
            + "\nmaxDoc= " + stats.maxDoc()
            + "\nsumDocFreq= " + stats.sumDocFreq()
            + "\nsumTotalFreq= " + stats.sumTotalTermFreq()
        );
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}
