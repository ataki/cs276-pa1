package cs276.assignments;

import cs276.util.Pair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Map;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

public class Index {

    // Term id -> (position in index file, doc frequency) dictionary
    private static Map<Integer, Pair<Long, Integer>> postingDict = new TreeMap<Integer, Pair<Long, Integer>>();
    // Doc name -> doc id dictionary
    private static Map<String, Integer> docDict = new TreeMap<String, Integer>();
    // Term -> term id dictionary
    private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
    // Block queue
    private static LinkedList<File> blockQueue = new LinkedList<File>();

    // Total file counter
    private static int totalFileCount = 0;
    // Document counter
    private static int docIdCounter = 0;
    // Term counter
    private static int wordIdCounter = 0;
    // Index
    private static BaseIndex index = null;


    /*
     * Write a posting list to the file
     * You should record the file position of this posting list
     * so that you can read it back during retrieval
     *
     * */
    private static void writePosting(FileChannel fc, PostingList posting, boolean isFinalIteration)
            throws IOException {

        long pos = fc.position();
        int termId = posting.getTermId();

        /*
            Since we use writePosting for writing intermediate results as well as final index,
            use this boolean to signal when we're writing to final index
         */
        if (isFinalIteration) {
            Pair<Long, Integer> pair;
            if (!postingDict.containsKey(termId)) {
                pair = new Pair<Long, Integer>(pos, posting.getList().size());
            } else {
                pair = postingDict.get(termId);
                pair.setFirst(pos);
                pair.setSecond(posting.getList().size());
            }
            postingDict.put(termId, pair);
        }

        index.writePosting(fc, posting);
    }

    public static void main(String[] args) throws IOException {
        /* Parse command line */
        if (args.length != 3) {
            System.err
                    .println("Usage: java Index [Basic|VB|Gamma] data_dir output_dir");
            return;
        }

        /* Get index */
        String className = "cs276.assignments." + args[0] + "Index";
        try {
            Class<?> indexClass = Class.forName(className);
            index = (BaseIndex) indexClass.newInstance();
        } catch (Exception e) {
            System.err
                    .println("Index method must be \"Basic\", \"VB\", or \"Gamma\"");
            throw new RuntimeException(e);
        }

        /* Get root directory */
        String root = args[1];
        File rootdir = new File(root);
        if (!rootdir.exists() || !rootdir.isDirectory()) {
            System.err.println("Invalid data directory: " + root);
            return;
        }

        /* Get output directory */
        String output = args[2];
        File outdir = new File(output);
        if (outdir.exists() && !outdir.isDirectory()) {
            System.err.println("Invalid output directory: " + output);
            return;
        }

        if (!outdir.exists()) {
            if (!outdir.mkdirs()) {
                System.err.println("Create output directory failure");
                return;
            }
        }

        /* BSBI indexing algorithm */
        File[] dirlist = rootdir.listFiles();

        /* For each block */
        for (File block : dirlist) {
            File blockFile = new File(output, block.getName());
            blockQueue.add(blockFile);

            File blockDir = new File(root, block.getName());
            File[] filelist = blockDir.listFiles();

            /*
                maintain a mapping of termId -> list of docId's for each block.
                BSBI specifies storing pairs, but for ease of conversion
                and representation, we use this mapping structure.
            */
            TreeMap<Integer, ArrayList<Integer>>postingLists = new TreeMap<Integer, ArrayList<Integer>>();

            /* For each file */
            for (File file : filelist) {
                ++totalFileCount;
                String fileName = block.getName() + "/" + file.getName();

                int docId = docIdCounter++;
                docDict.put(fileName, docId);

                BufferedReader reader = new BufferedReader(new FileReader(file));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] tokens = line.trim().split("\\s+");
                    for (String token : tokens) {
                        int tokenId;
                        if (!termDict.containsKey(token)) {
                            tokenId = wordIdCounter++;
                            termDict.put(token, tokenId);
                        } else {
                            tokenId = termDict.get(token);
                        }

                        ArrayList<Integer>curList;
                        if (!postingLists.containsKey(tokenId)) {
                            curList = new ArrayList<Integer>(100);
                            curList.add(docId);
                            postingLists.put(tokenId, curList);
                        } else {
                            curList = postingLists.get(tokenId);
                            curList.add(docId);
                        }
                    }
                }
                reader.close();
            }

            /* Sort and output */
            System.err.println("blockfile: " + blockFile.getName());
            if (!blockFile.createNewFile()) {
                System.err.println("Create new block failure.");
                return;
            }

            RandomAccessFile bfc = new RandomAccessFile(blockFile, "rw");
            Iterator<Map.Entry<Integer, ArrayList<Integer>>> it = postingLists.entrySet().iterator();

            /* TreeMap provides in-order traversal of keys */
            while (it.hasNext()) {
                Map.Entry<Integer, ArrayList<Integer>> pair = it.next();

                int termId = pair.getKey();
                ArrayList<Integer> docIdList = pair.getValue();
                Collections.sort(docIdList);

                PostingList plist = new PostingList(termId, docIdList);
                writePosting(bfc.getChannel(), plist, false);
            }

            bfc.close();
        }

        /* Required: output total number of files. */
        System.out.println(totalFileCount);

        /* Merge blocks */
        while (true) {
            if (blockQueue.size() <= 1)
                break;

            File b1 = blockQueue.removeFirst();
            File b2 = blockQueue.removeFirst();

            File combfile = new File(output, b1.getName() + "+" + b2.getName());
            if (!combfile.createNewFile()) {
                System.err.println("Create new block failure.");
                return;
            }

            RandomAccessFile bf1 = new RandomAccessFile(b1, "r");
            RandomAccessFile bf2 = new RandomAccessFile(b2, "r");
            RandomAccessFile mf = new RandomAccessFile(combfile, "rw");

            long lenbf1 = bf1.length();
            long lenbf2 = bf2.length();

            boolean isFinalIteration = blockQueue.size() == 0;

            /*
                Merge lazily loads posting lists from the two inverted blockfiles.
                It relies on the fact that both blockfiles are sorted by termId,
                and the list of docIds in each posting list is sorted.
             */
            PostingList plist1 = index.readPosting(bf1.getChannel());
            PostingList plist2 = index.readPosting(bf2.getChannel());
            while((bf1.getFilePointer() != lenbf1) && (bf2.getFilePointer() != lenbf2)) {
                int termId1 = plist1.getTermId();
                int termId2 = plist2.getTermId();
                if (termId1 == termId2) {
                    ArrayList<Integer> docIdList = new ArrayList<Integer>(plist1.getList());
                    docIdList.addAll(plist2.getList());
                    Collections.sort(docIdList);
                    PostingList mergedList = new PostingList(plist1.getTermId(), docIdList);
                    writePosting(mf.getChannel(), mergedList, isFinalIteration);
                    plist1 = index.readPosting(bf1.getChannel());
                    plist2 = index.readPosting(bf2.getChannel());
                } else if (termId1 < termId2) {
                    writePosting(mf.getChannel(), plist1, isFinalIteration);
                    plist1 = index.readPosting(bf1.getChannel()); // advance fp
                } else {
                    writePosting(mf.getChannel(), plist2, isFinalIteration);
                    plist2 = index.readPosting(bf2.getChannel()); // advance fp
                }
            }

            RandomAccessFile bfFinal = (bf1.getFilePointer() == lenbf1) ? bf2 : bf1;
            long lenbfFinal = (bfFinal == bf1) ? lenbf1 : lenbf2;
            while (bfFinal.getFilePointer() != lenbfFinal) {
                PostingList plist = index.readPosting(bfFinal.getChannel());
                writePosting(mf.getChannel(), plist, isFinalIteration);
            }

            bf1.close();
            bf2.close();
            mf.close();
            b1.delete();
            b2.delete();
            blockQueue.add(combfile);
        }

        /* Dump constructed index back into file system */
        File indexFile = blockQueue.removeFirst();
        indexFile.renameTo(new File(output, "corpus.index"));

        BufferedWriter termWriter = new BufferedWriter(new FileWriter(new File(
                output, "term.dict")));
        for (String term : termDict.keySet()) {
            termWriter.write(term + "\t" + termDict.get(term) + "\n");
        }
        termWriter.close();

        BufferedWriter docWriter = new BufferedWriter(new FileWriter(new File(
                output, "doc.dict")));
        for (String doc : docDict.keySet()) {
            docWriter.write(doc + "\t" + docDict.get(doc) + "\n");
        }
        docWriter.close();

        BufferedWriter postWriter = new BufferedWriter(new FileWriter(new File(
                output, "posting.dict")));
        for (Integer termId : postingDict.keySet()) {
            postWriter.write(termId + "\t" + postingDict.get(termId).getFirst()
                    + "\t" + postingDict.get(termId).getSecond() + "\n");
        }
        postWriter.close();
    }

}
