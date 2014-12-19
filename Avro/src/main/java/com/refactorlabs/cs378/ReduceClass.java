package com.refactorlabs.cs378;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Reduce class for wordstatisticsavro.  Extends class Reducer, provided by Hadoop.
 * This class defines the reduce() function for the word count example.
 * 
 * @author Renato John Recio
 * @author dfranke
 */
public class ReduceClass
		extends Reducer<Text, AvroValue<WordStatisticsData>,
		AvroKey<Pair<CharSequence, WordStatisticsData>>, NullWritable> {

	//Returns the variance from the document_count, mean, and frequency_squared parameters
		//Calculates using the formula E[x^2] - (E[x])^2
		public double getVariance(double document_count, double mean, double frequency_squared){
			double expected_value_1 = ((frequency_squared) / (document_count));
			double expected_value_2 = (mean * mean);
			double variance = expected_value_1 - expected_value_2;

			return variance;
		}

		//Returns the mean from the document_count and frequency parametes
		//Calculates using the formula frequency / number of documents
		public double getMean(double document_count, double frequency){
			double mean = frequency/document_count;
			return mean;

		}

		@Override
		public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
				throws IOException, InterruptedException {

			//Instantiate variables that will store our output values
			long document_count = 0, frequency = 0, frequency_squared = 0;

			// Sum up the counts for the current word, specified in object "key".
			
			//extract the values from the avro datum and store them in our
			//local variables
			for (AvroValue<WordStatisticsData> value : values) {
				document_count += value.datum().getDocumentCount();
				frequency += value.datum().getTotalCount();
				frequency_squared += value.datum().getSumOfSquares();
			}

			//Get the mean and variance for our word
			double mean = getMean(document_count, frequency);
			double variance = getVariance(document_count, mean, frequency_squared);

			
			//Construct our builder
			WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
			
			//Store our longs and doubles in the builder
			builder.setDocumentCount(document_count);
			builder.setTotalCount(frequency);
			builder.setSumOfSquares(frequency_squared);
			builder.setMean(mean);
			builder.setVariance(variance);
			

			//Write the output to context as avrokey
			context.write(
					new AvroKey<Pair<CharSequence, WordStatisticsData>>
							(new Pair<CharSequence, WordStatisticsData>(key.toString(), builder.build())),
					NullWritable.get());
		}
	
	
	
}

