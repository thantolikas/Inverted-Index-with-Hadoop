package nai;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MrMain {

	public static void main(String[] args) throws Exception {

		// Split the arguments
		Configuration invIndexconf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(invIndexconf, args).getRemainingArgs();
		int N, k, m;

		if (otherArgs.length != 5) {
			System.err.println("Usage: 5 arguments, <N> <k> <m> <input_path> <output_path>");
			System.exit(2);
		}

		// Initilize the variables
		N = Integer.valueOf(otherArgs[0]);
		k = Integer.valueOf(otherArgs[1]);
		m = Integer.valueOf(otherArgs[2]);
		String inputPath = otherArgs[3];
		String outputPath = otherArgs[4];
		VectorWritable.size = N;
		KMeans.k = k;
		ResultsPrinter.k = k;

		// Inverted Index job
		System.out.println("\n INVERTED INDEX \n\n");
		DistributedCache.addCacheFile(new URI(inputPath + "/stopwords.txt"), invIndexconf);
		Job invIndexJob = new Job(invIndexconf, "Inverted Index");
		invIndexJob.setJarByClass(MrMain.class);
		invIndexJob.setMapperClass(InvertedIndex.Map.class);
		invIndexJob.setCombinerClass(InvertedIndex.Reduce.class);
		invIndexJob.setReducerClass(InvertedIndex.Reduce.class);
		invIndexJob.setOutputKeyClass(Text.class);
		invIndexJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(invIndexJob, new Path(inputPath + "/docs"));
		FileOutputFormat.setOutputPath(invIndexJob, new Path(outputPath));
		invIndexJob.waitForCompletion(true);

		// K-Means job
		System.out.println("\n K-MEANS 0 \n\n");
		Configuration kMeansConf = new Configuration();
		DistributedCache.addCacheFile(new URI(inputPath + "/centers.txt"), kMeansConf);
		Job kMeansJob = new Job(kMeansConf, "K-means");
		kMeansJob.setJarByClass(MrMain.class);
		kMeansJob.setMapperClass(KMeans.Map.class);
		kMeansJob.setCombinerClass(KMeans.Reduce.class);
		kMeansJob.setReducerClass(KMeans.Reduce.class);
		kMeansJob.setOutputKeyClass(Text.class);
		kMeansJob.setOutputValueClass(VectorWritable.class);
		FileInputFormat.addInputPath(kMeansJob, new Path(outputPath));
		FileOutputFormat.setOutputPath(kMeansJob, new Path(outputPath + "/_kmeans"));
		kMeansJob.waitForCompletion(true);

		// K-Means job iterations
		for (int i = 0; i < m; i++) {
			System.out.println("\n K-MEANS " + (i + 1) + " \n\n");
			kMeansConf = new Configuration();
			if (i == 0)
				DistributedCache.addCacheFile(new URI(outputPath + "/_kmeans/part-r-00000"), kMeansConf);
			else
				DistributedCache.addCacheFile(new URI(outputPath + "/_kmeans/_iterations/" + (i) + "/part-r-00000"), kMeansConf);
			kMeansJob = new Job(kMeansConf, "K-means");
			kMeansJob.setJarByClass(MrMain.class);
			kMeansJob.setMapperClass(KMeans.Map.class);
			kMeansJob.setCombinerClass(KMeans.Reduce.class);
			kMeansJob.setReducerClass(KMeans.Reduce.class);
			kMeansJob.setOutputKeyClass(Text.class);
			kMeansJob.setOutputValueClass(VectorWritable.class);
			FileInputFormat.addInputPath(kMeansJob, new Path(outputPath));
			FileOutputFormat.setOutputPath(kMeansJob, new Path(outputPath + "/_kmeans/_iterations/" + (i + 1)));
			kMeansJob.waitForCompletion(true);
		}

		// Results Printer Job
		Configuration resutlsConf = new Configuration();
		DistributedCache.addCacheFile(new URI(outputPath + "/_kmeans/_iterations/" + m + "/part-r-00000"), resutlsConf);
		Job resutlsJob = new Job(resutlsConf, "Results");
		resutlsJob.setJarByClass(MrMain.class);
		resutlsJob.setMapperClass(ResultsPrinter.Map.class);
		resutlsJob.setCombinerClass(ResultsPrinter.Reduce.class);
		resutlsJob.setReducerClass(ResultsPrinter.Reduce.class);
		resutlsJob.setOutputKeyClass(Text.class);
		resutlsJob.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(resutlsJob, new Path(outputPath));
		FileOutputFormat.setOutputPath(resutlsJob, new Path(outputPath + "/_results"));
		resutlsJob.waitForCompletion(true);

		System.exit(0);
	}

}
