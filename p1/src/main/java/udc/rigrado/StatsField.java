package udc.rigrado;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
        try (StatsField statsField = new StatsField()) {
            statsField.readStats(indexPath, field);
        }
    }


    private void readStats(String indexPath, String field) throws IOException {
        if (indexPath == null) {
            System.err.println("No index path was given, the program can't continue");
            System.exit(1);
        }
        try {
            Directory indexDir = FSDirectory.open(Paths.get(indexPath));
            DirectoryReader indexReader = DirectoryReader.open(indexDir);
            IndexSearcher searcher = new IndexSearcher(indexReader);
            CollectionStatistics modelDescriptionStatistics = null;

            if (field != null)
                modelDescriptionStatistics = searcher.collectionStatistics(field);
//            else {
//                fields = doc.getFields();
//                // Note doc.getFields() gets the stored fields
//
//                for (IndexableField field : fields) {
//                    String fieldName = field.name();
//                    System.out.println(fieldName); //  + ": " + doc.get(fieldName)
//
//                }
//            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
//        System.out.println("\nmodel Description Statistics\n");
//        System.out.println("\nfield= " + modelDescriptionStatistics.field() + " docCount= "
//                + modelDescriptionStatistics.docCount() + " maxDoc= " + modelDescriptionStatistics.maxDoc()
//                + " sumDocFreq= " + modelDescriptionStatistics.sumDocFreq() + " sumTotalFreq= "
//                + modelDescriptionStatistics.sumTotalTermFreq());
    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}
