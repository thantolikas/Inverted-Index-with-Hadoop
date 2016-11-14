package nai;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndex {
	public static class Map extends Mapper<Object, Text, Text, Text> {

		private final Text fileNo = new Text();
		private final Text word = new Text();
		private final Text stemmedWord = new Text();

		private HashMap<String, Boolean> stopWordHash;
		private HashMap<String, Boolean> punctuationMarks;

		@Override
		protected void setup(Context context) {
			// Get the cached archives/files
			Path stopWordPath;

			stopWordHash = new HashMap<>();
			punctuationMarks = new HashMap<>();

			// Put all the stopwords in the stopWordsHash table
			try {
				stopWordPath = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];

				Scanner scanner = new Scanner(new File(stopWordPath.toString()));

				while (scanner.hasNext()) {
					String line = scanner.nextLine();

					stopWordHash.put(line, true);

				}
				scanner.close();

				// Put some extra words
				punctuationMarks.put(".", true);
				punctuationMarks.put("'", true);
				punctuationMarks.put(",", true);
				punctuationMarks.put("-", true);
				punctuationMarks.put("_", true);
				punctuationMarks.put("/", true);
				punctuationMarks.put("\"", true);
				punctuationMarks.put("\\", true);
				punctuationMarks.put("(", true);
				punctuationMarks.put(")", true);
				punctuationMarks.put("+", true);
				punctuationMarks.put("-", true);
				punctuationMarks.put("=", true);
				punctuationMarks.put("'", true);
				punctuationMarks.put(":", true);
				punctuationMarks.put("@", true);
				punctuationMarks.put("!", true);
				punctuationMarks.put("#", true);
				punctuationMarks.put("$", true);
				punctuationMarks.put("%", true);
				punctuationMarks.put("^", true);
				punctuationMarks.put("&", true);
				punctuationMarks.put("?", true);
				punctuationMarks.put("<", true);
				punctuationMarks.put(">", true);
				punctuationMarks.put("[", true);
				punctuationMarks.put("]", true);
				punctuationMarks.put("{", true);
				punctuationMarks.put("}", true);
				punctuationMarks.put("|", true);
				punctuationMarks.put(";", true);



			} catch (IOException e) { // TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			StringTokenizer itr = new StringTokenizer(value.toString());

			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());

				if (!StringUtils.isNumeric(word.toString())) { // Check if the word is not a number
					if (!stopWordHash.containsKey(word.toString()) && !punctuationMarks.containsKey(word.toString())) { // Check if the word is a stop word and more
						for (Entry<String, Boolean> entry : punctuationMarks.entrySet()) {

							String str = entry.getKey();

							// Check if the word has a stop character in the start or in the end
							if (word.toString().toCharArray()[0] == str.toCharArray()[0]) {
								word.set(word.toString().substring(1, word.toString().length()));
							}
							if (word.toString().toCharArray()[word.toString().length() - 1] == str.toCharArray()[0]) {
								if (word.toString().length() == 1) {
									return;
								}
								word.set(word.toString().substring(0, word.toString().length() - 1));
							}

						}

						// Stem the word
						Stemmer stemmer = new Stemmer();
						stemmer.add(word.toString().toCharArray(), word.toString().length());
						stemmer.stem();

						stemmedWord.set(stemmer.toString());

						// Get the file name, so that we know the team (0,1,2,3...)
						String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
						fileNo.set(fileName);

						// Write the word with the team
						context.write(stemmedWord, fileNo);


					}

				}

			}
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private final Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// Initialize the vector of the word
			int vector[] = new int[VectorWritable.size];
			for (int i = 0; i < VectorWritable.size; i++)
				vector[i] = 0;

			// Set the i position of the vector array to "1", if there is a "i" value in the values array
			for (Text val : values) {
				String[] tokens = val.toString().split(" ");
				if (tokens.length == 1) {
					vector[Integer.valueOf(tokens[0])] = 1;
				} else {
					int counter = 0;
					for (String id : tokens) {
						if (Integer.valueOf(id) == 1) {
							vector[counter] = 1;

						}
						counter++;
					}
				}
			}

			// Create a string with the vector, for the output
			StringBuilder t = new StringBuilder();
			for (int i = 0; i < VectorWritable.size; i++)
				t.append(vector[i] + " ");

			result.set(t.toString());

			context.write(key, result);
		}
	}

}
