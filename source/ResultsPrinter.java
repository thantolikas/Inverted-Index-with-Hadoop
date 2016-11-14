package nai;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class ResultsPrinter {

	public static int k;

	public static class Map extends Mapper<Object, Text, Text, Text> {
		VectorWritable current = new VectorWritable();
		Path centersFile;
		VectorWritable teams[];
		Text team = new Text();

		@Override
		protected void setup(Context context) throws IOException {

			teams = new VectorWritable[k];
			centersFile = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];

			// Read the teams from the last centers file
			Scanner scanner = new Scanner(new File(centersFile.toString()));
			int i = 0;
			while (scanner.hasNext()) {
				String line = scanner.nextLine();
				String[] tokens = line.toString().split(" |\t");

				VectorWritable temp = new VectorWritable();

				double[] v = new double[tokens.length-1];
				for(int j=0 ; j<v.length ; j++)
					v[j]=Double.valueOf(tokens[j+1]);

				temp.set(v);
				teams[i] = temp;
				i++;
			}
			scanner.close();
		}

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] tokens = value.toString().split(" |\t");

			double[] v = new double[tokens.length-1];
			for(int j=0 ; j<v.length ; j++)
				v[j]=Double.valueOf(tokens[j+1]);

			current.set(v);

			// Find the team that the word belongs to
			double max = VectorWritable.compare(current, teams[0]);
			int max_team = 0;

			for (int i = 1; i < k; i++) {

				if (max < VectorWritable.compare(current, teams[i])) {
					max = VectorWritable.compare(current, teams[i]);
					max_team = i;
				}

			}

			team.set(String.valueOf(max_team));

			// Map the team with the word
			context.write(team, new Text(tokens[0]));

		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// Just print the team with each word it has
			StringBuilder words = new StringBuilder();
			for (Text t : values) {
				words.append(t.toString() + " ");
			}
			context.write(key, new Text(words.toString()));
		}
	}

}
