package udc.rigrado;

import org.apache.lucene.index.IndexWriterConfig;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.IOUtils;

import java.nio.file.Path;
import java.util.concurrent.ExecutorService;

public class IndexMedline implements AutoCloseable{

    private final Path indexPath;

    private enum Modes {
        CREATE(IndexWriterConfig.OpenMode.CREATE),
        APPEND(IndexWriterConfig.OpenMode.APPEND),
        CREATE_OR_APPEND(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);

        private final IndexWriterConfig.OpenMode openMode;

        Modes(IndexWriterConfig.OpenMode openMode) {
            this.openMode = openMode;
        }
    }

    private IndexMedline(Path indexPath) throws IOException {
        this.indexPath = indexPath;
    }

    public static void main(String[] args) {
        String usage = "java IndexMedline"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-openmode append | create | create_or_append] [-indexingmodel jm lambda | tfidf]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index\n";
        String indexPath = "index";
        String docsPath = null;
        Modes openmode = Modes.CREATE;
        String mode = null;
        String indexingmode = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-indexingmodel":
                    indexingmode = args[++i];
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

        final Path docDir = Paths.get(docsPath);
        if (!Files.isReadable(docDir)) {
            System.out.println("Document directory '" + docDir.toAbsolutePath()
                    + "' does not exist or is not readable, please check the path");
            System.exit(1);
        }

        Date start = new Date();
        try {
            System.out.println("Indexing to directory '" + indexPath + "'...");

            Directory dir = FSDirectory.open(Paths.get(indexPath));
            Analyzer analyzer = new StandardAnalyzer();
            IndexWriterConfig iwc = new IndexWriterConfig(analyzer);

            iwc.setOpenMode(openmode.openMode);

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 IndexMedline indexFiles = new IndexMedline(Path.of(indexPath))) {
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
    void indexDocs (){

    }

    @Override
    public void close() throws IOException {
        IOUtils.close();
    }
}
