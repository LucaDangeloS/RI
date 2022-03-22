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
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.demo.knn.KnnVectorDict;
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

    private final KnnVectorDict vectorDict;
    private final ExecutorService executor;

    private IndexFiles(KnnVectorDict vectorDict, int numCores) throws IOException {
        this.vectorDict = vectorDict;
        this.executor = Executors.newFixedThreadPool(numCores);
    }

    /** Index all text files under a directory. */
    public static void main(String[] args) throws Exception {
        String usage = "java org.apache.lucene.demo.IndexFiles"
                + " [-index INDEX_PATH] [-docs DOCS_PATH] [-update] [-numThreads NUM_THREADS]\n\n"
                + "This indexes the documents in DOCS_PATH, creating a Lucene index"
                + "in INDEX_PATH that can be searched with SearchFiles\n";
        String indexPath = "index";
        String docsPath = null;
        int threads = Runtime.getRuntime().availableProcessors();
        boolean create = true;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-index":
                    indexPath = args[++i];
                    break;
                case "-docs":
                    docsPath = args[++i];
                    break;
                case "-update":
                    create = false;
                    break;
                case "-create":
                    create = true;
                    break;
                case "-numThreads":
                    threads = Integer.parseInt(args[++i]);
                    break;
                default:
                    throw new IllegalArgumentException("unknown parameter " + args[i]);
            }
        }

        if (docsPath == null) {
            System.err.println("Usage: " + usage);
            System.exit(1);
        }

        if (threads <= 0) {
            System.err.println("Thread numbers must be a non-zero positive integer.");
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

            if (create) {
                // Create a new index in the directory, removing any
                // previously indexed documents:
                iwc.setOpenMode(OpenMode.CREATE);
            } else {
                // Add new documents to an existing index:
                iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
            }

            // Optional: for better indexing performance, if you
            // are indexing many documents, increase the RAM
            // buffer. But if you do this, increase the max heap
            // size to the JVM (eg add -Xmx512m or -Xmx1g):
            //
            // iwc.setRAMBufferSizeMB(256.0);

            try (IndexWriter writer = new IndexWriter(dir, iwc);
                 IndexFiles indexFiles = new IndexFiles(null, threads)) {
                indexFiles.indexDocs(writer, docDir);

                // NOTE: if you want to maximize search performance,
                // you can optionally call forceMerge here. This can be
                // a terribly costly operation, so generally it's only
                // worth it when your index is relatively static (ie
                // you're done adding documents to it):
                //
                // writer.forceMerge(1);
            } finally {
                IOUtils.close();
            }

            Date end = new Date();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                System.out.println("Indexed " + reader.numDocs() + " documents in " + (end.getTime() - start.getTime())
                        + " milliseconds");
                if (reader.numDocs() > 100 && System.getProperty("smoketester") == null) {
                    throw new RuntimeException(
                            "Are you (ab)using the toy vector dictionary? See the package javadocs to understand why you got this exception.");
                }
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
    void indexDocs(IndexWriter writer, Path docDir) throws IOException {
        final Runnable worker = new ThreadedIndex(writer, docDir, this.executor);
        executor.execute(worker);


        /* Wait up to 1 minute to finish all the previously submitted jobs */

        try {
            executor.awaitTermination(20, TimeUnit.SECONDS);
            executor.shutdownNow(); //garantee shutdown
        } catch (final InterruptedException e) {
            e.printStackTrace();
            System.exit(-2);
        }
        System.out.println("All threads finished");
    }

    public static class ThreadedIndex implements Runnable {

        private final Path path;
        private final IndexWriter writer;
        private final ExecutorService executor;
        private static volatile int threadCount = 1;
        static final FieldType TYPE_STORED_INDEXED = new FieldType();
        static {
            TYPE_STORED_INDEXED.setStored(true);
            TYPE_STORED_INDEXED.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        }

        public ThreadedIndex(IndexWriter writer, final Path folder, ExecutorService executor) {
            this.writer = writer;
            this.path = folder;
            this.executor = executor;
        }

        synchronized private void incrementThreadCount(){
            threadCount++;
        }

        synchronized private void decrementThreadCount(){
            threadCount--;
        }



        /** Indexes a single document */
        void indexDoc(IndexWriter writer, Path file) throws IOException {
            try (InputStream stream = Files.newInputStream(file)) {
                // make a new, empty document
                Document doc = new Document();

                // Add the path of the file as a field named "path". Use a
                // field that is indexed (i.e. searchable), but don't tokenize
                // the field into separate words and don't index term frequency
                // or positional information:
                Field pathField = new StringField("path", file.toString(), Field.Store.YES);
                doc.add(pathField);

                // Add the last modified date of the file a field named "modified".
                // Use a LongPoint that is indexed (i.e. efficiently filterable with
                // PointRangeQuery). This indexes to milli-second resolution, which
                // is often too fine. You could instead create a number based on
                // year/month/day/hour/minutes/seconds, down the resolution you require.
                // For example the long value 2011021714 would mean
                // February 17, 2011, 2-3 PM.
                BasicFileAttributes attr = Files.readAttributes(file, BasicFileAttributes.class);
                FileTime lastModified = attr.lastModifiedTime();
                FileTime creationTime = attr.creationTime();
                FileTime lastAccessTime = attr.lastAccessTime();
                FileTime lastModifiedTime = attr.lastModifiedTime();

                doc.add(new LongPoint("modified", lastModified.toMillis()));
                doc.add(new TextField("creationTime", creationTime.toString(), Field.Store.YES));
                doc.add(new TextField("lastAccessTime", lastAccessTime.toString(), Field.Store.YES));
                doc.add(new TextField("lastModifiedTime", lastModifiedTime.toString(), Field.Store.YES));

                doc.add(new TextField("creationTimeLucene",
                        DateTools.dateToString(new Date(creationTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));
                doc.add(new TextField("lastAccessTimeLucene",
                        DateTools.dateToString(new Date(lastAccessTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));
                doc.add(new TextField("lastModifiedTimeLucene",
                        DateTools.dateToString(new Date(lastModifiedTime.toMillis()), DateTools.Resolution.SECOND), Field.Store.YES));

                // Add the contents of the file to a field named "contents". Specify a Reader,
                // so that the text of the file is tokenized and indexed, but not stored.
                // Note that FileReader expects the file to be in UTF-8 encoding.
                // If that's not the case searching for special characters will fail.
                doc.add(new TextField("contents",
                        new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));

                doc.add(new TextField("contentsStored",
                        new String(stream.readAllBytes(), StandardCharsets.UTF_8), Field.Store.YES));

                doc.add(new TextField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
                doc.add(new TextField("thread", Thread.currentThread().getName(), Field.Store.YES));
//                doc.add(new TextField("type", Thread.currentThread().getName(), Field.Store.YES));
                doc.add(new FloatPoint("sizeKB", (float) Files.size(file)/1024));

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
            try {
                DirectoryStream<Path> directoryStream = Files.newDirectoryStream(path);

                for (final Path subpath : directoryStream) {
                    if (Files.isDirectory(subpath)) {
                        final Runnable worker = new ThreadedIndex(this.writer, subpath, this.executor);
                        incrementThreadCount();
                        this.executor.execute(worker);
                    } else {
                        indexDoc(writer, subpath);
                    }
                }
                decrementThreadCount();

                if (threadCount == 0) {
                    executor.shutdown();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(vectorDict);
    }
}


