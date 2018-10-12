 java class com.kai.exercise.largeFileSorting.LargeFileSorting
 
 1. Usage:
 1) function "sortWordsInfile",
    It can be used to sort all the words in an extremely large input file and list all the distinct words in order, separated by spaces in a output file.
 2) "main" function
    Command line Usage: java com/kai/exercise/largeFileSorting/LargeFileSorting -b <block size> -m <max fds> -t <tempory file dir> <inputFile.txt> <outputFile.txt>

 2. Assumptions:
 1) The words and their comparison are based on default Character Set, default natural comparison.
 2) The available space of the file system is sufficient, though it doesn't need be larger than the size of the input file.
 3) The input file contains English words in regular format. However, words like "back-end" will be parsed as two words, so is words like "o'clock".

 3. Consideration:
 1) Memory Block Size
    The input file can be times larger than the size of available memory
    but the sorting should be done in memory as much as it's possible to reach best possible performance;
    so the parameter for the size of the memory block is introduced.
 2) Max fds
    The merge will be performed on multiple input streams; however, limited by the Operating System,
    the user can be limited to a maximum number of how many file descriptors can be opened at the same time.
    So, the parameter for the maximum number of simultaneously opened files is introduced.

 4. Solution:
    1) Sort segments of the input file in memory blocks and save the word-sorted blocks in temporary files;
    2) When the number of temporary files reaches the designated maximum, merge sort these files into one file;
    3) Continuously do 1 and 2 until all words in the input file have been processed;
    4) The last step is to merge multiple temporary files into the output one.
    5) Use buffer reader to read files.
    6) Use priority queue to facility the merge sorting of multiple sorted input streams

 5. Future improvement
    1) In a multi-core environment, it will benefit the performance to sort blocks in parallel.
    2) Elaborate the Reqular Expression used by the word scanner to parse words more accurately, such words like "o'clock".
       But it may significantly impact the performance though.
