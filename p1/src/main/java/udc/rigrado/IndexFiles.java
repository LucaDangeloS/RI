package udc.rigrado;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
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

/**
 * Index all text files under a directory.
 *
 * <p>
 * This is a command-line application demonstrating simple Lucene indexing. Run
 * it with no command-line arguments for usage information.
 */
public class IndexFiles implements AutoCloseable {

    private final ExecutorService executor;
    private final Path indexPath;
    private static final String DEFAULT_PROPERTIES_PATH = "./src/main/resources/config.properties";
    private final Properties properties = new Properties();
    private enum Modes {
        CREATE(OpenMode.CREATE),
        APPEND(OpenMode.APPEND),
        CREATE_OR_APPEND(OpenMode.CREATE_OR_APPEND);

        private final OpenMode openMode;

        Modes(OpenMode openMode) {
            this.openMode = openMode;
        }
    }

    private IndexFiles(int numCores, Path indexPath) throws IOException {
        try {
            properties.load(new FileInputStream(DEFAULT_PROPERTIES_PATH));
            System.out.println("Properties file loaded successfully!");
        } catch (IOException e) {
            System.err.println("Error reading properties file: " + e);
        }
        this.executor = Executors.newFixedThreadPool(numCores);
        this.indexPath = indexPath;
    }

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage = "java IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-numThreads NUM_THREADS] [-openmode append | create | create_or_append]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index\n";
        String indexPath = "index";
        String docsPath = null;
        boolean partialIndex = false;
        boolean update = false;
        int threads = Runtime.getRuntime().availableProcessors();
        Modes openmode = Modes.CREATE;
        String mode = null;
        int depth_int = -1;
        String depth = null;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-partialIndex":
                    partialIndex = true;
                    break;
                case "-update":
                    update = true;
                    break;
                case "-deep":
                    depth = args[++i];
                    break;
                case "-numThreads":
                    threads = Integer.parseInt(args[++i]);
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

        if (threads <= 0) {
            System.err.println("Threads number must be a non-zero positive integer.");
            System.exit(1);
        }

        if (mode != null) {
            try { openmode = Modes.valueOf(mode.toUpperCase()); }
            catch (IllegalArgumentException e) {
                System.err.println("Openmode must be append, create or create_or_append");
                System.exit(1);
            }
        }

        if (depth != null) {
            try {
                depth_int = Integer.parseInt(depth);
                if (depth_int < 0) throw new IllegalArgumentException();
            } catch (Exception e) {
                System.err.println("Depth value must be a zero or positive integer.");
                System.exit(1);
            }
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
                    IndexFiles indexFiles = new IndexFiles(threads, Path.of(indexPath))) {
                indexFiles.indexDocs(writer, docDir, depth_int, partialIndex, update);
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

    // Auxiliary structure to ease passing down information to the threads
    public static class IndexInfo {
        final ExecutorService executor;
        final boolean update;
        final Set<String> fileTypesList;
        final boolean onlyTopLines;
        final int topLines;
        final boolean onlyBottomLines;
        final int bottomLines;

        public IndexInfo(ExecutorService executor, boolean update, Properties prop) {
            this.executor = executor;
            this.update = update;
            String tmp = prop.getProperty("onlyFiles");
            if (tmp != null) {
                String[] fileTypesSplit = tmp.split(" ");
                this.fileTypesList = Set.of(fileTypesSplit);
            } else this.fileTypesList = null;
            tmp = prop.getProperty("onlyTopLines");
            if (this.onlyTopLines = (tmp != null)) {
                this.topLines = Integer.parseInt(tmp);
            } else {this.topLines = -1;}
            tmp = prop.getProperty("onlyBottomLines");
            if (this.onlyBottomLines = (tmp != null)) {
                this.bottomLines = Integer.parseInt(tmp);
            } else {this.bottomLines = -1;}
        }
    }

    /**
     * Indexes the given file using the given writer, or if a directory is given,
     * recurses over files and directories found under the given directory.
     *
     * <p>
     * NOTE: This method indexes one document per input file. This is slow. For good
     * throughput, put multiple documents into your input file(s). An example of
     * this is in the benchmark module, which can create "line doc" files, one
     * document per line, using the <a href=
     * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
     * >WriteLineDocTask</a>.
     *
    * @param writer Writer to the index where the given file/dir info will be
     *               stored
    * @param docDir   The file to index, or the directory to recurse into to find
     *               files to indt
    * @throws IOException If there is a low-level I/O error
     */
    void indexDocs(IndexWriter writer, Path docDir, int depth, boolean partialIndex, boolean update) throws IOException {
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir);
        ArrayList<IndexWriter> partialWriters = new ArrayList<>();
        IndexInfo ii = new IndexInfo(executor, update, this.properties);

        // No se indexa nada
        if (depth == 0) return;

        for (final Path subpath : directoryStream) {
            if (Files.isDirectory(subpath)) {
                final Runnable worker;

                if (partialIndex) {
                    Analyzer analyzer =  new StandardAnalyzer();
                    IndexWriterConfig partialIwc = new IndexWriterConfig(analyzer);
                    partialIwc.setOpenMode(writer.getConfig().getOpenMode());
                    Directory partialDIr = FSDirectory.open(Paths.get(indexPath + "-" + subpath.getFileName()));
                    IndexWriter partialWriter = new IndexWriter(partialDIr, partialIwc);

                    partialWriters.add(partialWriter);

                    worker = new ThreadedIndex(partialWriter, subpath, ii,depth-1);
                } else
                    worker = new ThreadedIndex(writer, subpath, ii, depth-1);
                executor.execute(worker);
            }
        }

        /* Wait up to 1 minute to finish all the previously submitted jobs */
        try {
            executor.awaitTermination(60, TimeUnit.MINUTES);
            executor.shutdownNow(); //garantee shutdown
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        if (partialIndex)
            for (IndexWriter iw: partialWriters) {
                iw.commit();
                iw.close();
                writer.addIndexes(iw.getDirectory());
            }

        System.out.println("All threads finished");
    }

    public static class ThreadedIndex implements Runnable {

        private final Path path;
        private final IndexWriter writer;
        private final int depth;
        private final ExecutorService executor;
        private final IndexInfo info;
        private static volatile int threadCount = 0;
        static final FieldType TYPE_STORED_INDEXED = new FieldType();
        static {
            TYPE_STORED_INDEXED.setStored(true);
            TYPE_STORED_INDEXED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
            TYPE_STORED_INDEXED.setStoreTermVectors(true);
            TYPE_STORED_INDEXED.setStoreTermVectorPositions(true);
            TYPE_STORED_INDEXED.freeze();
        }
        // Non-partial index threaded index
        public ThreadedIndex(IndexWriter writer, final Path folder,
                             IndexInfo info, int depth) {
            this.writer = writer;
            this.path = folder;
            this.executor = info.executor;
            this.depth = depth;
            this.info = info;
        }

        synchronized private void incrementThreadCount(){ threadCount++; }

        synchronized private void decrementThreadCount(){ threadCount--; }

        synchronized private int readThreadCount() { return threadCount; }

        /** Indexes a single document */
        void indexDoc(IndexWriter writer, Path file) throws IOException {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();
                OpenMode openmode = writer.getConfig().getOpenMode();

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                FileTime lastModified = attr.lastModifiedTime();
                FileTime creationTime = attr.creationTime();
                FileTime lastAccessTime = attr.lastAccessTime();
                FileTime lastModifiedTime = attr.lastModifiedTime();
                float size = (float) Files.size(file)/1024;

                String filetype;
                if (attr.isRegularFile())
                    filetype = "RegularFile";
                else if (attr.isSymbolicLink())
                    filetype = "SymbolicLink";
                else if (attr.isDirectory())
                    filetype = "Directory";
                else filetype = "other";

                doc.add(new LongPoint("modified", lastModified.toMillis()));
                doc.add(new StringField("creationTime", creationTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
                doc.add(new StringField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));
                doc.add(new StringField("creationTimeLucene",
                        DateTools.dateToString(new Date(creationTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));
                doc.add(new StringField("lastAccessTimeLucene",
                        DateTools.dateToString(new Date(lastAccessTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));
                doc.add(new StringField("lastModifiedTimeLucene",
                        DateTools.dateToString(new Date(lastModifiedTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));
                doc.add(new TextField("contents",
                        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
                doc.add(new TextField("contentsStored",
                        new String(stream.readAllBytes(), StandardCharsets.UTF_8), Field.Store.YES));
                doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
                doc.add(new StringField("type", filetype, Field.Store.YES));
                doc.add(new StringField("SizeKBStored", String.valueOf(size), Field.Store.YES));
                doc.add(new FloatPoint("sizeKB", size));

                if (info.onlyBottomLines && info.topLines > 0) {
                    Stream<String> lines = Files.lines(file);
                    doc.add(new TextField("onlyTopLines",
                            lines.limit(info.topLines+1).collect(Collectors.joining()), Field.Store.YES));
                    lines.close();
                }
                if (info.onlyTopLines && info.bottomLines > 0) {
                    Stream<String> lines = Files.lines(file);
                    long len = lines.count();
                    lines = Files.lines(file);
                    final String lastLines = lines.skip(len - info.bottomLines).collect(Collectors.joining());
                    doc.add(new TextField("onlyBottomLines", lastLines, Field.Store.YES));
                    lines.close();
                }

                switch (openmode){
                    case CREATE:
                        System.out.println("Adding " + file);
                        writer.addDocument(doc);
                        break;
                    case APPEND:
                        if (info.update) {
//                            IndexSearcher searcher = IndexSearcher();
//                            TopDocs results = searcher.search(new TermQuery(new Term("path", file.toString())), 1);
//                            if (results.totalHits > 0){
//                                writer.updateDocument(new Term("path", file.toString()), doc);
//                            }
                            System.out.println("Updating " + file);
                            writer.updateDocument(new Term("path", file.toString()), doc);
                        } else {
                            // Acepta duplicados
                            System.out.println("Adding " + file);
                            writer.addDocument(doc);
                        }
                        break;
                    case CREATE_OR_APPEND:
                        System.out.println("Indexing " + file);
                        writer.updateDocument(new Term("path", file.toString()), doc);
                        break;
                    default:
                        break;
                }
            }
        }

        @Override
        public void run() {
            incrementThreadCount();
            // Iterate over directory, indexing files and making recursive calls with directories

            try {
                DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);

                for (final Path subpath : directoryStream) {
                    if (Files.isDirectory(subpath)) {
                        if (this.depth == 0) break;
                        final Runnable worker =
                                new ThreadedIndex(this.writer, subpath, this.info, this.depth-1);
                        this.executor.execute(worker);
                    } else {
                        if (info.fileTypesList == null)
                            indexDoc(writer, subpath);
                        else {
                            String file_str = subpath.getFileName().toString();
                            int i = file_str.lastIndexOf(".");
                            String extension = file_str.substring(i);
                            if (info.fileTypesList.contains(extension))
                                indexDoc(writer, subpath);
                        }
                    }
                }
                decrementThreadCount();

                if (readThreadCount() == 0) {
                    executor.shutdown();
                }
            } catch (IOException e) {
                decrementThreadCount();
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close();
    }
}


