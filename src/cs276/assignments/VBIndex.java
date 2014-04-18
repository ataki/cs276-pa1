package cs276.assignments;

import java.nio.channels.FileChannel;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class VBIndex implements BaseIndex {
	/*
			Encode gaps between postings in postings-lists. Our implementation
			uses 4 extra bytes to encode the byte length of each posting list
			to allow easy buffer reading.
	*/

  public int INVALID_VBCODE = -1;

	@Override
	public PostingList readPosting(FileChannel fc) {
		try {
			/* read termId / doc frequency, and bytes needed to decode */
			ByteBuffer bb = ByteBuffer.allocate(12);

			int bytesRead = fc.read(bb);
			if (bytesRead == -1) {
				System.err.println("VB: readPosting read fewer than 8 bytes from fc");
				return null;	
			}
			bb.rewind();

			int termId = bb.getInt();
			int docFreq = bb.getInt();
			int numBytes = bb.getInt(); 

			// read postings list into fixed byte array 
			ByteBuffer docBuf = ByteBuffer.allocate(numBytes);
			bytesRead = fc.read(docBuf);
			if (bytesRead == -1) {
				System.err.println("VB: readPosting read fewer than 8 bytes from fc");
				return null;
			}
			docBuf.rewind();
			byte[] input = new byte[numBytes];
			docBuf.get(input);

			// translate postings list
			int[] list = new int[numBytes];

			// tuple of (decodedGap, index);
			int[] numberEndIndex = new int[2];
			int nWritten = 0;
			int startIndex = 0;
			while (docFreq != 0) {
				decodeInteger(input, startIndex, numberEndIndex);
				list[nWritten] = numberEndIndex[0];
				startIndex = numberEndIndex[1];
				docFreq--;	
			}

      GapDecode(list);

			ArrayList<Integer> finalList = new ArrayList<Integer>(numBytes);
			for (int i = 0; i < numBytes; i++) {
				finalList.add(list[i]);	
			}

			return new PostingList(termId, finalList);

		} catch (Exception e) {
			System.err.println("VB ReadPosting Error: " + e.toString());
			return null;	
		}
	}

	@Override
	public void writePosting(FileChannel fc, PostingList p) {
		try {
			int termId = p.getTermId();
      List<Integer> docList = p.getList();
			int[] gapList = new int[docList.size()];

      for (int i = 0; i < docList.size(); i++)
        gapList[i] = docList.get(i);

      GapEncode(gapList);

			ByteBuffer bb = ByteBuffer.allocate(12);
			byte[] gapOutput = new byte[4 * gapList.length];
      int accumBytes = 0;

			// VBEncode the postinglist
      for (int aGapList : gapList)
      	accumBytes += encodeInteger(aGapList, gapOutput);

      // Write buffers out  
      bb.putInt(termId);
      bb.putInt(gapList.length);
      bb.putInt(accumBytes);
			bb.flip();

      fc.write(bb);

      ByteBuffer gapBuf = ByteBuffer.wrap(gapOutput);
      gapBuf.flip();
			while (gapBuf.hasRemaining()) {
				int numWritten = fc.write(gapBuf);
			}

		} catch (IOException e) {
      System.err.println("VB Error: " + e.toString());
    }
	}

  private void GapEncode(int[] docIdList) {
    for (int i = docIdList.length - 1; i > 0; i--) {
        docIdList[i] -= docIdList[i-1];
    }
  }

  private void GapDecode(int[] gapIdList) {
    for (int i = 1; i < gapIdList.length; i++) {
        gapIdList[i] += gapIdList[i-1];
    }
  }

	private int encodeInteger(int gap, byte[] outputVBCode) {
		int numBytes = 0;

    // put lower order bytes in highest position in outputVBCode
    // the do-while is for the case where gap == 0
    do {
      byte b = (byte)0;
      if (numBytes == 0) {
          b |= 0x80;
      }
      b |= (byte)(gap & 0x7f);
      outputVBCode[numBytes] = b;
      numBytes++;
      gap >>= 7;
    } while (gap > 0);

    // reverse lower order
    for (int i = 0; i < numBytes / 2; i++) {
      byte temp = outputVBCode[i];
      outputVBCode[i] = outputVBCode[numBytes-i-1];
      outputVBCode[numBytes-i-1] = temp;
    }
		return numBytes;
	}

	private void decodeInteger(byte[] inputVBCode, int startIndex, int[] numberEndIndex) {
    int output = 0;
    int lastIndex = startIndex;

    // decode and accumulate powers
    while ((inputVBCode[lastIndex] & 0x80) == 0 && (lastIndex - startIndex < 4)) {
      output |= (inputVBCode[lastIndex]);
      output <<= 7;
      lastIndex++;
    }

    // accumulate 4 lower-order bits
    if ((inputVBCode[lastIndex] & 0x80) != 0) {
      output |= 0xFF & (inputVBCode[lastIndex] & ~0x80);
      lastIndex++;
    } else {
      output = INVALID_VBCODE;
      lastIndex = 0;
    }

    numberEndIndex[0] = output;
    numberEndIndex[1] = lastIndex;
	}
}
