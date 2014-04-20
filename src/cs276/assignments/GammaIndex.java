package cs276.assignments;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class GammaIndex implements BaseIndex {

    @Override
    public PostingList readPosting(FileChannel fc) {
        try {
            // read termId / doc frequency, and bytes needed to decode 
            ByteBuffer bb = ByteBuffer.allocate(12);

            int bytesRead = fc.read(bb);
            if (bytesRead == -1) {
                System.err.println("Gamma: readPosting read fewer than 8 bytes from fc");
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

            BitSet inputBitSet = convertToBitSet(input);

            // translate postings list
            int[] list = new int[docFreq];

            // tuple of (decodedGap, index);
            int[] numberEndIndex = new int[2];
            int nWritten = 0;
            int startIndex = 0;

            for (int i = docFreq; i > 0; i--) {
                GammaDecodeInteger(inputBitSet, startIndex, numberEndIndex);
                list[nWritten++] = numberEndIndex[0];
                startIndex = numberEndIndex[1];
            }

            GapDecode(list);

            ArrayList<Integer> finalList = new ArrayList<Integer>(numBytes);
            for (int i = 0; i < docFreq; i++) {
                finalList.add(list[i]);
            }
            return new PostingList(termId, finalList);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public void writePosting(FileChannel fc, PostingList p) {
        try {
            int termId = p.getTermId();
            List<Integer> docList = p.getList();
            int[] gapList = new int[docList.size()];
            // convert postings list to int[]
            for (int i = 0; i < docList.size(); i++) 
                gapList[i] = docList.get(i);

            GapEncodeGamma(gapList);

            ByteBuffer bb = ByteBuffer.allocate(12);
            BitSet gapOutput = new BitSet();
            gapOutput.set(0, false);

            int accumBits = 0;

            for (int gap : gapList) {
                accumBits = GammaEncodeInteger(gap, gapOutput, accumBits);
            }
            accumBits -= 1;

            byte[] gapOutputByteBuffer = gapOutput.toByteArray();

            // Handle special case when we're encoding a streak
            // of 1's, e.g. [X, Y, 1, 1, 1, 1, ...]. These ones
            // won't show up in the BitSet encoding as they're all
            // set to 0
            if (accumBits % 8 != 0) {
                int numBytesTotal = (accumBits + 8 - 1) / 8;
                int numBytesNeeded = numBytesTotal - gapOutputByteBuffer.length;
                if (numBytesNeeded > 0) {
                    byte[] temp = new byte[numBytesTotal];
                    byte[] toConcat = new byte[numBytesNeeded];
                    initializeToZeroes(toConcat);
                    concatArrays(temp, gapOutputByteBuffer, toConcat);
                    gapOutputByteBuffer = temp;
                }
            }

            // in case of encoding 1, we have to manually set the all 0's buffer
            int accumBytes = gapOutputByteBuffer.length;

            // Write buffer out
            bb.putInt(termId);
            bb.putInt(gapList.length);
            bb.putInt(accumBytes);
            bb.flip();
            fc.write(bb);

            ByteBuffer gapBuf = ByteBuffer.wrap(gapOutputByteBuffer);
            gapBuf.limit(accumBytes);
            gapBuf.position(0);
            while (gapBuf.hasRemaining()) {
                int numWritten = fc.write(gapBuf);
            }

        } catch (IOException e) {
            System.err.println("Gamma WritePosting Error: " + e.toString());
        }
    }

    private void initializeToZeroes(byte[] arr) {
        for (int i = 0; i < arr.length; i++) 
            arr[i] = 0;
    }

    private void concatArrays(byte[] out, byte[] a, byte[] b) {
        System.arraycopy(a, 0, out, 0, a.length);
        System.arraycopy(b, 0, out, a.length, b.length);
    }

    private void GapEncodeGamma(int[] docIdList) {
        for (int i = docIdList.length - 1; i > 0; i--) {
            docIdList[i] -= docIdList[i-1];
        }
    }

    private void GapDecode(int[] gapIdList) {
        for (int i = 1; i < gapIdList.length; i++) {
            gapIdList[i] += gapIdList[i-1];
        }
    }

    private BitSet convertToBitSet(byte[] input) {
        BitSet bs = new BitSet();
        for (int i = 0; i < input.length; i++) {
            byte b = input[i];
            for (int j = 0; j < 8; j++) {
                bs.set(8 * i + j, (b & (1 << j)) != 0);
            }
        }
        return bs;
    }

    /**
     * Gamma encodes number.  The encoded bits are placed in BitSet outputGammaCode starting at
     * (0-based) index position startIndex.  Returns the index position immediately following the
     * encoded bits.  If you try to gamma encode 0, then the return value should be startIndex (i.e.,
     * it does nothing).
     *
     * @param number            Number to be gamma encoded
     * @param outputGammaCode   Gamma encoded bits are placed in this BitSet starting at startIndex
     * @param startIndex        Encoded bits start at this index position in outputGammaCode
     * @return                  Index position in outputGammaCode immediately following the encoded bits
     */
    private int GammaEncodeInteger(int number, BitSet outputGammaCode, int startIndex) {
        int nextIndex = startIndex;

        if (number != 0) {
            // find left-most bit
            int leftMostOneBit = 31;
            int mask = 1 << 31;
            while ((mask & number) == 0) {
                leftMostOneBit--;
                mask >>>= 1;
            }

            // compose length
            nextIndex = UnaryEncodeInteger(leftMostOneBit, outputGammaCode, nextIndex);

            // compose offset from leftMostBit
            for (int i = 0; i < leftMostOneBit; i++) {
                int curBit = leftMostOneBit - 1 - i;
                outputGammaCode.set(nextIndex + i, ((1 << curBit) & number) != 0);
            }

            // compose length using unary converter
            nextIndex += (leftMostOneBit);
        }
        return nextIndex;
    }

    /**
     * Decodes the Gamma encoded number in BitSet inputGammaCode starting at (0-based) index startIndex.
     * The decoded number is returned in numberEndIndex[0] and the index position immediately following
     * the encoded value in inputGammaCode is returned in numberEndIndex[1].
     *
     * @param inputGammaCode  BitSet containing the gamma code
     * @param startIndex      Gamma code starts at this index position
     * @param numberEndIndex  Return values: index 0 holds the decoded number; index 1 holds the index
     *                        position in inputGammaCode immediately following the gamma code.
     */
    private void GammaDecodeInteger(BitSet inputGammaCode, int startIndex, int[] numberEndIndex) {
        int decodedIndex[] = new int[2];

        // decode length
        UnaryDecodeInteger(inputGammaCode, startIndex, decodedIndex);

        int length = decodedIndex[0];
        int num = 0;
        int nextIndex = decodedIndex[1];

        // decode offset
        for (int i = 0; i < length; i++)
            if (inputGammaCode.get(nextIndex + i))
                num |= (1 << (length - 1 - i));
        num |= (1 << length);

        numberEndIndex[0] = num;
        numberEndIndex[1] = nextIndex + length;
    }


    public static int UnaryEncodeInteger(int number, BitSet outputUnaryCode, int startIndex) {
        int nextIndex = startIndex;
        for (int i = 0; i < number; i++) {
            outputUnaryCode.set(nextIndex + i);
        }
        outputUnaryCode.set(nextIndex + number, false);
        nextIndex += (number + 1);
        return nextIndex;
    }


    public static void UnaryDecodeInteger(BitSet inputUnaryCode, int startIndex, int[] numberEndIndex) {
        int num = 0;
        int index = startIndex;
        while (inputUnaryCode.get(index)) {
            num++;
            index++;
        }
        numberEndIndex[0] = num;
        numberEndIndex[1] = index + 1;
    }
}
