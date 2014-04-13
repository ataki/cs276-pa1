package cs276.assignments;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.security.KeyException;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.List;

public class Query {

	// Term id -> position in index file
	private static Map<Integer, Long> posDict = new TreeMap<Integer, Long>();
	// Term id -> document frequency
	private static Map<Integer, Integer> freqDict = new TreeMap<Integer, Integer>();
	// Doc id -> doc name dictionary
	private static Map<Integer, String> docDict = new TreeMap<Integer, String>();
	// Term -> term id dictionary
	private static Map<String, Integer> termDict = new TreeMap<String, Integer>();
	// Index
	private static BaseIndex index = null;

	
	/* 
	 * Write a posting list with a given termID from the file 
	 * You should seek to the file position of this specific
	 * posting list and read it back.
	 * */
	private static PostingList readPosting(FileChannel fc, int termId)
			throws IOException {
        if (!posDict.containsKey(termId)) return null;
        long pos = posDict.get(termId);
        fc.position(pos);
        return index.readPosting(fc);
	}

	public static void main(String[] args) throws IOException {
		/* Parse command line */
		if (args.length != 2) {
			System.err.println("Usage: java Query [Basic|VB|Gamma] index_dir");
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

		/* Get index directory */
		String input = args[1];
		File inputdir = new File(input);
		if (!inputdir.exists() || !inputdir.isDirectory()) {
			System.err.println("Invalid index directory: " + input);
			return;
		}

		/* Index file */
		RandomAccessFile indexFile = new RandomAccessFile(new File(input,
				"corpus.index"), "r");

		String line = null;
		/* Term dictionary */
		BufferedReader termReader = new BufferedReader(new FileReader(new File(
				input, "term.dict")));
		while ((line = termReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			termDict.put(tokens[0], Integer.parseInt(tokens[1]));
		}
		termReader.close();

		/* Doc dictionary */
		BufferedReader docReader = new BufferedReader(new FileReader(new File(
				input, "doc.dict")));
		while ((line = docReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			docDict.put(Integer.parseInt(tokens[1]), tokens[0]);
		}
		docReader.close();

		/* Posting dictionary */
		BufferedReader postReader = new BufferedReader(new FileReader(new File(
				input, "posting.dict")));
		while ((line = postReader.readLine()) != null) {
			String[] tokens = line.split("\t");
			posDict.put(Integer.parseInt(tokens[0]), Long.parseLong(tokens[1]));
			freqDict.put(Integer.parseInt(tokens[0]),
					Integer.parseInt(tokens[2]));
		}
		postReader.close();

		/* Processing queries */
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		/* For each query */
		while ((line = br.readLine()) != null) {
            String tokens[] = line.trim().split("//s+");

            /*
                Maintain a final list of doc ids that represent the
                intersection of all queries. For each token, intersect
                its list with this "final list".
            */

            ArrayList<Integer> finalDocIdList = new ArrayList<Integer>();
            boolean emptyResult = false;

            for (String token: tokens) {
                List<Integer> nextDocIdList;
                if (termDict.containsKey(token)) {
                    int tokenId = termDict.get(token);
                    // TODO Catch KeyNotFoundException for tokenId
                    indexFile.seek(0);
                    PostingList pl = readPosting(indexFile.getChannel(), tokenId);
                    // TODO Catch when pl == null (empty index)
                    nextDocIdList = pl.getList();
                } else {
                    emptyResult = true;
                    break;
                }

                /* Special case the first query since we're intersecting with nothing */
                if (finalDocIdList.size() == 0) {
                    finalDocIdList.addAll(nextDocIdList);
                }

                /* Our familiar  */
                else {
                    int idx1 = 0;
                    int idx2 = 0;

                    ArrayList<Integer> intersect = new ArrayList<Integer>();
                    while ((idx1 != finalDocIdList.size()) && (idx2 != nextDocIdList.size())) {
                        int docId1 = finalDocIdList.get(idx1);
                        int docId2 = nextDocIdList.get(idx2);
                        if (docId1 == docId2) {
                            intersect.add(docId1);
                            idx1++;
                            idx2++;
                        } else if (docId1 > docId2) {
                            idx2++;
                        } else {
                            idx1++;
                        }
                    }
                    finalDocIdList.clear();
                    finalDocIdList.addAll(intersect);
                }
            }

            /* Print Results */
            if (emptyResult) {
                System.out.println("no results found");
            } else {
                for (Integer docId : finalDocIdList) {
                    String docName = docDict.get(docId);
                    System.out.println(docName);
                }
            }
		}
		br.close();
		indexFile.close();
	}
}
