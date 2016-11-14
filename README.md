# Inverted-Index-with-Hadoop
Create an inverted index using k-means in Hadoop

Execution guidelines

Input: 
1. The text files
2. The number of thematic groups
3. The number of iterations
4. The input path
5. The output path

The input path should containt an "input" folder which consists of:
1. The text file with the centers named "centers"
2. The "stopwords" text file
3. A folder named "docs" containing the text files named 0...N-1

The files should not containt ".txt" in their names

The "centers" file should be of this form:
<group> <vector>
e.g 3 0 1 0 1 0

Example of execution:

hadoop jar /home/kds.jar 5 5 10 /user/hduser/project/input
/user/hduser/project/output

*See the commited input folder as an example*
