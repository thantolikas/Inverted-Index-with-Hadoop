package nai;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class VectorWritable implements Writable {


	DoubleWritable[] values;
	public static int size;

	public VectorWritable() {
		values = new DoubleWritable[size];
		for (int i = 0; i < size; i++) {
			values[i] = new DoubleWritable(i);
		}

	}


	public void set(double[] values) {
		for(int i=0 ; i<size ; i++)
			this.values[i].set(values[i]);

	}

	public DoubleWritable[] getValues() {
		return values;
	}

	public static double compare(VectorWritable v1, VectorWritable v2) {
		double sum1 = 0, sum2 = 0, sum3 = 0;

		// Compare the vectors with the given algorithm
		for (int i = 0; i < size; i++) {
			sum1 += v1.getValues()[i].get() * v2.getValues()[i].get();
			sum2 += v1.getValues()[i].get() * v1.getValues()[i].get();
			sum3 += v2.getValues()[i].get() * v2.getValues()[i].get();
		}
		sum2 = Math.sqrt(sum2);
		sum3 = Math.sqrt(sum3);

		return sum1 / (sum2 * sum3);
	}

	@Override
	public void write(DataOutput out) {
		try {
			for (int i = 0; i < size; i++) {
				values[i].write(out);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		for(int i=0 ; i<size ; i++)
			values[i].readFields(in);
	}

	public static VectorWritable read(DataInput in) throws IOException {
		VectorWritable w = new VectorWritable();
		w.readFields(in);
		return w;
	}

	@Override
	public String toString() {
		StringBuilder temp = new StringBuilder();

		for(int i=0 ; i<size ; i++)
			temp.append(values[i] + " ");

		return temp.toString();

	}
}
