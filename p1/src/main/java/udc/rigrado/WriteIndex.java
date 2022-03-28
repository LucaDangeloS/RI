package udc.rigrado;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.io.IOException;
import java.io.FileWriter;
import java.nio.file.Paths;
import java.util.List;

public class WriteIndex implements AutoCloseable {

    public static void main(String[] args) throws Exception{
        String usage = "java WriteIndex"
                + " [-index INDEX_PATH] [-outputfile FILE_PATH]\n\n";
        String indexPath = null;
        String outputfile = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-outputfile":
                    outputfile = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter " + args[i]);
            }
        }

        if(indexPath == null || outputfile == null){
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        try ( WriteIndex writeIndex = new WriteIndex() ){
            writeIndex.write(indexPath, outputfile);
        }
    }



    private void write(String indexPath, String outputfile) throws Exception{
        Directory dir = null;
        DirectoryReader indexReader = null;
        Document doc = null;
        List<IndexableField> fields = null;

        if (indexPath == null) {
            System.err.println("No index path was given, the program can't continue");
            System.exit(1);
        }

        try {
            dir = FSDirectory.open(Paths.get(indexPath));
            indexReader = DirectoryReader.open(dir);
        } catch (IOException e1) {
            System.out.println("Graceful message: exception " + e1);
            e1.printStackTrace();
        }

        if(indexReader != null){

            try {
                FileWriter myWriter = new FileWriter(outputfile);
                for (int i = 0; i < indexReader.numDocs(); i++) {
                    try {
                        doc = indexReader.document(i);
                    } catch (IOException e1) {
                        System.out.println("Graceful message: exception " + e1);
                        e1.printStackTrace();
                    }
                    if(doc != null){
                        myWriter.append("\nDocument " + i + "\n");
                        fields = doc.getFields();
                        for (IndexableField field : fields) {
                            String fieldName = field.name();
                            if(fieldName.compareTo("contentsStored") == 0){
                              myWriter.append( fieldName + ": \n" + doc.get(fieldName)  +"\n" );
                            }else{
                                myWriter.append( fieldName + ": " + doc.get(fieldName) + "\n\n");
                            }
                        }
                    }
                }
                myWriter.close();
                System.out.println("Document created succesfully in path " + outputfile + "\n");

            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }


        }

    }

    @Override
    public void close() throws Exception {
        IOUtils.close();
    }
}
