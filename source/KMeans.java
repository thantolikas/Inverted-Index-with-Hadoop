package nai;

import java.io.File;
import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeans {

	public static int k;

	public static class Map extends Mapper<Object, Text, Text, VectorWritable> {
		VectorWritable current = new VectorWritable();
		Path centersFile;
		VectorWritable teams[];
		Text team = new Text();

		@Override
		protected void setup(Context context) throws IOException {

			// Initiate the teams array
			teams = new VectorWritable[k];
			centersFile = DistributedCache.getLocalCacheFiles(context.getConfiguration())[0];
			System.out.println("Centers file: " + centersFile.toString());

			// Read each team from the file
			Scanner scanner = new Scanner(new File(centersFile.toString()));
			int i = 0;
			while (scanner.hasNext()) {

				String line = scanner.nextLine();
				System.out.println("Line: " + line);
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

			// Split the line to tokens, which are the word and it's vector
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
			context.write(team, current);

		}
	}

	public static class Reduce extends Reducer<Text, VectorWritable, Text, VectorWritable> {
		VectorWritable outValue = new VectorWritable();

		@Override
		public void reduce(Text key, Iterable<VectorWritable> values, Context context) throws IOException, InterruptedException {

			// Initialize variables
			int counter = 0;
			double[] sums = new double[VectorWritable.size];
			for (int i = 0; i < VectorWritable.size; i++) {
				sums[i] = 0;
			}

			// Add all the vectors
			for (VectorWritable current : values) {
				for (int i = 0; i < VectorWritable.size; i++) {
					sums[i] += current.getValues()[i].get();
				}
				counter++;
			}

			// And find their mean vector
			for (int i = 0; i < VectorWritable.size; i++) {
				sums[i] /= counter;
			}

			outValue.set(sums);
			context.write(key, outValue);
		}
	}

}
