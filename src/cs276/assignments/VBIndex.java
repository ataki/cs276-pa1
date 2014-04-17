package cs276.assignments;

import java.nio.channels.FileChannel;

public class VBIndex implements BaseIndex {

	@Override
	public PostingList readPosting(FileChannel fc) {
		try {
			ByteBuffer bb = ByteBuffer.allocate(8);

			int bytesRead = fc.read(bb);
			if (bytesRead == -1) {
				System.err.println("VB: readPosting read fewer than 8 bytes from fc");
				return null;	
			}
			bb.rewind();

			
		} catch (Exception e) {
			return null;	
		}
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		/*
		 * Your code here
		 */
	}
}
