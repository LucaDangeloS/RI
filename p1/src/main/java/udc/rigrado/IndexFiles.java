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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
    private enum modes {
        CREATE("create", OpenMode.CREATE),
        APPEND("append", OpenMode.APPEND),
        CREATE_OR_APPEND("create_or_append", OpenMode.CREATE_OR_APPEND);

        private final String text;
        private final OpenMode openMode;

        modes(String string, OpenMode openMode) {
            this.text = string;
            this.openMode = openMode;
        }
        @Override
        public String toString() {
            return this.text;
        }
    }

    private IndexFiles(int numCores, Path indexPath) throws IOException {
        this.executor = Executors.newFixedThreadPool(numCores);
        this.indexPath = indexPath;
    }

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-numThreads NUM_THREADS] [-openmode append | create | create_or_append]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles\n";
        String indexPath = "index";
        String docsPath = null;
        boolean partialIndex = false;
        int threads = Runtime.getRuntime().availableProcessors();
        boolean update = false;
        String mode = null;
        modes openmode = modes.CREATE;
        String depth = null;
        int depth_int = -1;

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
            try { openmode = modes.valueOf(mode); }
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
                indexFiles.indexDocs(writer, docDir, depth_int, partialIndex, update); //TODO check if it's needed
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
        // Crea una tarea para el pool de threads por cada carpeta que encuentra en el path pasado
        DirectoryStream<Path> directoryStream = Files.newDirectoryStream(docDir);
        ArrayList<IndexWriter> partialWriters = new ArrayList<>();

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
                    worker = new ThreadedIndex(partialWriter, subpath, executor, depth - 1, update);
                } else
                    worker = new ThreadedIndex(writer, subpath, executor, depth - 1, update);
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
        private final boolean update;
        private final ExecutorService executor;
        private static volatile int threadCount = 0;
        static final FieldType TYPE_STORED_INDEXED = new FieldType();
        static {
            TYPE_STORED_INDEXED.setStored(true);
            TYPE_STORED_INDEXED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        }
        // Non-partial index threaded index
        public ThreadedIndex(IndexWriter writer, final Path folder, ExecutorService executor, int depth, boolean update) {
            this.writer = writer;
            this.path = folder;
            this.executor = executor;
            this.depth = depth;
            this.update = update;
        }

        synchronized private void incrementThreadCount(){ threadCount++; }

        synchronized private void decrementThreadCount(){ threadCount--; }

        synchronized private int readThreadCount() { return threadCount; }

        /** Indexes a single document */
        void indexDoc(IndexWriter writer, Path file) throws IOException {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();

                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                FileTime lastModified = attr.lastModifiedTime();
                FileTime creationTime = attr.creationTime();
                FileTime lastAccessTime = attr.lastAccessTime();
                FileTime lastModifiedTime = attr.lastModifiedTime();
                float size = (float) Files.size(file)/1024;

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
//                doc.add(new StringField("type", Thread.currentThread().getName(), Field.Store.YES)); //TODO add file type
                doc.add(new StringField("storedSizeKB", String.valueOf(size), Field.Store.YES));
                doc.add(new FloatPoint("sizeKB", size));

                if (writer.getConfig().getOpenMode() == OpenMode.CREATE) {
                    // New index, so we just add the document (no old document can be there):
                    System.out.println(Thread.currentThread().getName() + " adding " + file);
                    writer.addDocument(doc);
                } else {
                    // Existing index (an old copy of this document may have been indexed) so
                    // we use updateDocument instead to replace the old one matching the exact
                    // path, if present:
                    System.out.println("updating " + file);
                    writer.updateDocument(new Term("path", file.toString()), doc);
                }
            }
        }

        /**
         * This is the work that the current thread will do when processed by the pool.
         * In this case, it will only print some information.
         */
        @Override
        public void run() {
            incrementThreadCount();
            try {
                DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);

                for (final Path subpath : directoryStream) {

                    if (Files.isDirectory(subpath)) {
                        if (this.depth == 0) break;
                        final Runnable worker =
                                new ThreadedIndex(this.writer, subpath, this.executor, this.depth - 1, this.update);
                        this.executor.execute(worker);
                    } else {
                        indexDoc(writer, subpath);
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


