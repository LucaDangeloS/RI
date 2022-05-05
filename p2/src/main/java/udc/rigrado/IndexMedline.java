package udc.rigrado;

import org.apache.lucene.index.IndexWriterConfig;

import java.io.*;
import java.nio.file.*;
import java.util.*;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.nio.file.Path;

public class IndexMedline implements AutoCloseable{

    private enum Modes {
        CREATE(IndexWriterConfig.OpenMode.CREATE),
        APPEND(IndexWriterConfig.OpenMode.APPEND),
        CREATE_OR_APPEND(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        private final IndexWriterConfig.OpenMode openMode;

        Modes(IndexWriterConfig.OpenMode openMode) {
            this.openMode = openMode;
        }
    }

    private IndexMedline() throws IOException {
    }

    public static void main(String[] args) {
        String usage = "java IndexMedline"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-openmode append | create | create_or_append] [-indexingmodel jm lambda | tfidf]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index\n";
        String indexPath = "index";
        String docsPath = null;
        Modes openmode = Modes.CREATE;
        String mode = null;
        String indexingmodel = null;
        float lambda = 0;
        Similarity similarity;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-indexingmodel":
                    indexingmodel = args[++i];
                    if(Objects.equals(indexingmodel.toUpperCase(), "JM")){
                        lambda = Float.parseFloat(args[++i]);
                    }
                    break;
                case "-openmode":
                    mode = args[++i];
                    break;
                default:
                    throw new IllegalArgumentException("Unknown parameter " + args[i]);
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (indexingmodel == null) {
            System.err.println("Indexingmodel must be jm lambda or tfidf");
            System.exit(1);
        }

        if (mode != null) {
            try { openmode = Modes.valueOf(mode.toUpperCase()); }
            catch (IllegalArgumentException e) {
                System.err.println("Openmode must be append, create or create_or_append");
                System.exit(1);
            }
        }

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        switch (indexingmodel.toUpperCase()){
            case "JM":
                similarity = new LMJelinekMercerSimilarity(lambda);
                break;
            case "TFIDF":
                similarity = new ClassicSimilarity();
                break;
            default:
                throw new IllegalArgumentException("Invalid indexingmodel: " + indexingmodel);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer).setSimilarity(similarity);

            iwc.setOpenMode(openmode.openMode);

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 IndexMedline indexFiles = new IndexMedline()) {
                indexFiles.indexDocs(writer, docDir);
            } finally {
                IOUtils.close();
            }

            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                        + " milliseconds");
            }
        } catch (IOException e) {
            System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
        }
    }
    void indexDocs (IndexWriter writer, Path file) throws IOException {
        try (InputStream stream = Files.newInputStream(file)) {
            // make a new, empty document
            String str = new String(stream.readAllBytes());
            Document doc;
            OpenMode openmode = writer.getConfig().getOpenMode();
            String[] docsMED = str.split(".I ");
            String docMED;
            String[] Id_Content;
            String content;
            String Id;

            for ( int i = 1 ; i < docsMED.length; i++) {
                doc = new Document();
                docMED = docsMED[i];
                Id_Content = docMED.split("(\r)?\n.W(\r)?\n");
                Id = Id_Content[0];
                content = Id_Content[1];
                doc.add(new TextField("contents", content, Field.Store.YES));
                doc.add(new TextField("DocIDMedline", Id, Field.Store.YES));

                switch (openmode) {
                    case CREATE:
                        System.out.println("Adding " + Id);
                        writer.addDocument(doc);
                        break;
                    case APPEND:
                        System.out.println("Updating " + Id);
                        writer.addDocument(doc);
                        break;
                    case CREATE_OR_APPEND:
                        System.out.println("Indexing " + Id);
                        writer.updateDocument(new Term("DocIDMedline", Id), doc);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close();
    }
}
