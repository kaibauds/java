package com.kai.exercise.largeFileSorting;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Scanner;

/**
 *
 * java class com.kai.exercise.largeFileSorting.LargeFileSorting
 * 
 * 1. Usage:
 * 1) function "sortWordsInfile", 
 *    It can be used to sort all the words in an extremely large input file and list all the distinct words in order, separated by spaces in a output file. 
 * 2) "main" function
 *    Command line Usage: java com/kai/exercise/largeFileSorting/LargeFileSorting -b <block size> -m <max fds> -t <tempory file dir> <inputFile.txt> <outputFile.txt>
 * 
 * 2. Assumptions:
 * 1) The words and their comparison are based on default Character Set, default natural comparison. 
 * 2) The available space of the file system is sufficient, though it doesn't need be larger than the size of the input file.
 * 3) The input file contains English words in regular format. However, words like "back-end" will be parsed as two words, so is words like "o'clock". 
 * 
 * 3. Consideration:
 * 1) Memory Block Size
 *    The input file can be times larger than the size of available memory 
 *    but the sorting should be done in memory as much as it's possible to reach best possible performance;
 *    so the parameter for the size of the memory block is introduced.
 * 2) Max fds
 *    The merge will be performed on multiple input streams; however, limited by the Operating System, 
 *    the user can be limited to a maximum number of how many file descriptors can be opened at the same time.
 *    So, the parameter for the maximum number of simultaneously opened files is introduced.
 * 
 * 4. Solution:
 *    1) Sort segments of the input file in memory blocks and save the word-sorted blocks in temporary files;
 *    2) When the number of temporary files reaches the designated maximum, merge sort these files into one file;
 *    3) Continuously do 1 and 2 until all words in the input file have been processed;
 *    4) The last step is to merge multiple temporary files into the output one.
 *    5) Use buffer reader to read files.
 *    6) Use priority queue to facility the merge sorting of multiple sorted input streams
 * 
 * 5. Future improvement
 *    1) In a multi-core environment, it will benefit the performance to sort blocks in parallel.
 *    2) Elaborate the Reqular Expression used by the word scanner to parse words more accurately, such words like "o'clock".
 *       But it may significantly impact the performance though.
 *       
 */
public class LargeFileSorting {

	private static void displayUsage() {
		System.out.println("Options are:");
		System.out.println("-b (an integer): specify the number of Kbytes of memory used for in-memory sort, must be between 1 to 512. Default 16, i.e. 16K");
		System.out.println("-m (an integer): The maxium number of temporary files that can be opened at the same time. The minum is 2. Default 64.");
		System.out.println("-t (a string): The path of the directory where tmp files are to be stored. Default:\".\"");
	}

	/**
	 * 
	 * @param args
	 *
	 * [-b <blockSize> ] [ -m <maxfds> ] [-t <directory path> ] <input file name> <output file name>
	 *        
	 * default: -b 16  -m 64 -t "."
	 *        
	 * @throws IOException
	 */
	public static void main(final String[] args) throws IOException {
		String inputFileName = null, outputFileName = null;

		int blockSize = 16 * 1024;  //default -b
		int maxfds = 64;  //default -m 
		String tmpDir = ".";  //default -t
		
		for (int param = 0; param < args.length; ++param) {
			if (args[param].equals("-m") && args.length > param + 1) {
				param++;
				maxfds = Integer.parseInt(args[param]);
				if (maxfds < 2) {
					System.err.println("Invalid maximum of the number of auxiliary temporary files");
					displayUsage();
					return;
				}
			} else if (args[param].equals("-t") && args.length > param + 1) {
				param++;
				tmpDir = args[param];
				if (!new File(tmpDir).isDirectory()) {
					System.err.println("The directory doesn't exist");
					displayUsage();
					return;
				}
			} else if (args[param].equals("-b") && args.length > param + 1) {
				int ks=0;
				param++;
				ks = Integer.parseInt(args[param]);
				blockSize= ks*1024;
				if (ks < 0 || ks > 512) {
					System.err.println("Invalid nubmer for the size of the memeory");
					displayUsage();
					return;
				}
			} else {
				if (inputFileName == null) {
					inputFileName = args[param];
				} else if (outputFileName == null) {
					outputFileName = args[param];
				} else {
					System.out.println("Unparsed: " + args[param]);
				}
			}
		}
		if (outputFileName == null) {
			System.out.println("please provide input and output file names");
			displayUsage();
			return;
		}
		
		
		// done parsing the args, call the major function now
		
		sortWordsInFile(inputFileName, outputFileName, tmpDir, blockSize, maxfds);
		return;
	}

	/**
	 * 
	 * @param inputFileName    The path name of the input file.
	 * @param outputFileName   Tha path name of the output file. 
	 * @param tmpDirName       The path name of the directory where temporary files can be stored.
	 * @param blockSize        The rough size of memory block used for quickly sorting the contained word list.
	 * @param maxfds           The maximum number of the file descriptors simultaneously openned for this file sorting task.
	 * @throws IOException
	 */
	public static void sortWordsInFile(String inputFileName, String outputFileName, String tmpDirName, int blockSize,
			final int maxfds) throws IOException {

		List<File> tmpFiles = new ArrayList<>();
		//Use estimated value of maximum number of words a block can contain, because continuously count the precise number is an unnecessary cost. 
		int maxWords = blockSize / averageWordLength, blockCount = 0, wordCount = 0;
		Scanner scanner = new Scanner(new File(inputFileName));
		
		Scanner wordScanner = scanner.useDelimiter("[^A-Za-z]+"); //Assume words are seperated by non-letter characters.
		File mergedTmpFile = null, tmpDir = new File(tmpDirName);
		List<String> wordList = new ArrayList<>();
		
		try {
			while (wordScanner.hasNext()) {   //Process the input file word after word to the end
  			if (blockCount == maxfds) {
                   /* 
                    * Merge all the "maxfds" number of file into one since more blocks are coming ...
                    */	
					File tmpfile = File.createTempFile("sort", "tmp", tmpDir);
					LargeFileSorting.mergeSortedFiles(tmpFiles, tmpfile, false);
					tmpFiles.clear();
					tmpFiles.add(tmpfile);
					blockCount = 1;
				}
				wordCount = 0;
				while ( wordCount < maxWords && wordScanner.hasNext() ) {   // Prepare the block
					wordList.add(wordScanner.next());
					wordCount++;
				}
				tmpFiles.add(sortWordsInMem(wordList, mergedTmpFile = File.createTempFile("sort", "tmp", tmpDir))); // sort the block and save it in a temporary file
				mergedTmpFile.deleteOnExit();
				wordList.clear();
				blockCount++;
			}
			if (wordList.size() > 0) {
				tmpFiles.add(sortWordsInMem(wordList, tmpDir));
				wordList.clear();
			}

		} finally {
			scanner.close();
			wordScanner.close();
		}

		mergeSortedFiles(tmpFiles, new File(outputFileName));
		return;
	}

	/**
	 * @param sortedPeekableReaderList
	 * @param sortedFileWriter
	 * @param isFinalMerge
	 * @throws IOException
	 */
	
	private static void mergeSortedFiles(List<PeekableReader> sortedPeekableReaderList, BufferedWriter sortedFileWriter, 
			boolean isFinalMerge) throws IOException {

		/* Put all the peekable sorted input streams in the priority queue, 
		 * so that these input streams are automatically sorted in such a way that the sorted stream polled out can always give the 
		 * word that is prior to all other words in all the streams; but also, when the polled out stream is put back to the priority queue,
		 * the queue is re-sorted automatically. 
		 */
		PriorityQueue<PeekableReader> readerPQ = new PriorityQueue<>(11, new Comparator<PeekableReader>() {
			@Override
			public int compare(PeekableReader i, PeekableReader j) {
				return wordComparator.compare(i.peek(), j.peek());
			}
		});
		for (PeekableReader sortedPeekableReader : sortedPeekableReaderList) {
			if (!sortedPeekableReader.isEmpty()) {
				readerPQ.add(sortedPeekableReader);
			}
		}
		try {

			String lastWord = "";
			while (readerPQ.size() > 0) {
				PeekableReader sortedPeekableReader = readerPQ.poll();
				String r = sortedPeekableReader.poll();
				if (wordComparator.compare(r, lastWord) != 0) {
					sortedFileWriter.write(r);
					if (isFinalMerge)
						sortedFileWriter.write(" "); // The words are seperated by space in the output file.
					else
						sortedFileWriter.newLine();
					lastWord = r;
				}
				if (sortedPeekableReader.isEmpty()) {
					sortedPeekableReader.reader.close();
				} else {
					readerPQ.add(sortedPeekableReader); // Put back the input stream
				}
			}
		} finally {
			sortedFileWriter.close();
			for (PeekableReader sortedFileReader : readerPQ) {
				sortedFileReader.close();
			}
		}
		return;

	}

	private static void mergeSortedFiles(List<File> files, File outputFile) throws IOException {
		mergeSortedFiles(files, outputFile, true);
		return;
	}

	private static void mergeSortedFiles(List<File> files, File outputFile, boolean isFinalMerge) throws IOException {
		ArrayList<PeekableReader> sortedPeekableReaderList = new ArrayList<>();
		for (File f : files) {
			InputStream in = new FileInputStream(f);
			BufferedReader br;
			br = new BufferedReader(new InputStreamReader(in));

			PeekableReader sortedFileReader = new PeekableReader(br);
			sortedPeekableReaderList.add(sortedFileReader);
		}
		BufferedWriter sortedFileWriter = new BufferedWriter(
				new OutputStreamWriter(new FileOutputStream(outputFile, false)));
		mergeSortedFiles(sortedPeekableReaderList, sortedFileWriter, isFinalMerge);
		for (File f : files) {
			f.delete();  //delete all the temporary files
		}
		return;
	}

	/**
	 * Sort the words in a memory block (represented by List of String), and save the sorted distinct words in a temporary file.
	 * These words are separated by a new line in the temporary file for the convenient access by later process.
	 * 
	 * @param wordList
	 * @param outputFile
	 * @return
	 * @throws IOException
	 */
	private static File sortWordsInMem(List<String> wordList, File outputFile) throws IOException {
		Collections.sort(wordList, wordComparator);
		OutputStream out = new FileOutputStream(outputFile);
		try (BufferedWriter sortedFileWriter = new BufferedWriter(new OutputStreamWriter(out))) {
			String lastWord = null;
			Iterator<String> i = wordList.iterator();
			if (i.hasNext()) {
				lastWord = i.next();
				sortedFileWriter.write(lastWord);
				sortedFileWriter.newLine();
			}
			while (i.hasNext()) {
				String r = i.next();
				if (wordComparator.compare(r, lastWord) != 0) {
					sortedFileWriter.write(r);
					sortedFileWriter.newLine();
					lastWord = r;
				}
			}
		}
		return outputFile;
	}

	private static Comparator<String> wordComparator = new Comparator<String>() {
		@Override
		public int compare(String r1, String r2) {
			return r1.compareTo(r2);
		}
	};

	private static int averageWordLength = 7;

}

/**
 * 
 * Keep the first word of the input stream accessible, 
 * so that multiple streams represented by this class can be compared to each other by their on-top-of-the-stream word. 
 * 
 */
final class PeekableReader {
	public PeekableReader(BufferedReader r) throws IOException {
		this.reader = r;
		reload();
	}

	public String peek() {
		return this.peekable;
	}

	public String poll() throws IOException {
		String answer = peek().toString();
		reload();
		return answer;
	}

	public boolean isEmpty() {
		return this.peekable == null;
	}

	public void close() throws IOException {
		this.reader.close();
	}

	private void reload() throws IOException {
		this.peekable = this.reader.readLine();
	}

	public BufferedReader reader;

	private String peekable;

}
