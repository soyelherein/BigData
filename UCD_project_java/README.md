<h2>BigData basics using Java -College Project-UCD</h2>

<h4>Here we will try to solve very priliminary Bigdata problems using java, stated below:</h4>
<p>
Upload the 6 books in your HDFS and compute the following statistics on the corpus

1. (One single job) what is the number of distinct words in the corpus? how many
words start with the letter Z/z? how many words appear less than 4 times? Think
counters, and in particular user-defined counters. 

2. (One single job) how many terms appear in only one single document? Such words
may appear multiple times in one document, but they have to appear in only one
document in the corpus.

3. (One single job) Take one stopword (e.g., the, and) and compute the five words that
appear the most after it. E.g. "the cat belongs to the old lady from the hamlet" !
"cat ", "old" and "hamlet" would be candidates. The output should contains 5 lines
with the words and their frequency.
</p>

<h4>Solution:</h4>
<p>

1.Download the pg*.txt files into a directory from resources folder.</br>
2.create a directory in hadoop filesystem
	hadoop fs -mkdir input</br>
3.verify if the directory has been created using hadoop fs -ls command</br>
4.copy the files in newly created hdfs directory
       hadoop fs -put /home/user1/input/* input</br>
5.Verify the files in hadoop filesystem
       hadoop fs -ls input</br>
netbeans:</br>
6.Load the project in netbeans using import Bigdata.zip if you are downloading the zip file</br>
7.click on clean and build</br>
8. from the output window copy the jar file location
/home/user1/NetBeansProjects/BigData/dist/BigData.jar</br>
9. run the jar file from terminal as below
hadoop jar <jar file location>  <packagename.main class> <input_dir> <output dir>
hadoop jar /home/user1/NetBeansProjects/BigData/dist/BigData.jar wordcount.Q1JobControl input out</br>
10. to rerun remove the <output dir> from hadoop filesystem
hadoop fs -rm -r  -f out
</p>


