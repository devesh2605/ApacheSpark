import java.util.ArrayList;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class RDDMultiplication {

	private static final JavaSparkContext context;

	private static final FlatMapFunction<Tuple2<Double, Double>, Double> RESULT = new FlatMapFunction<Tuple2<Double, Double>, Double>() {
		
		private static final long serialVersionUID = 1L;

		@Override
		public Iterable<Double> call(Tuple2<Double, Double> value) throws Exception {
			ArrayList<Double> list = new ArrayList<Double>();
			list.add(value._1 * value._2);

			return list;
		}

	};

	private static final PairFunction<Double, Integer, Double> DOUBLE_VALUES = new PairFunction<Double, Integer, Double>() {
		
		private static final long serialVersionUID = 1L;
		int i = 0;

		@Override
		public Tuple2<Integer, Double> call(Double s) throws Exception {
			return new Tuple2<Integer, Double>(i++, s);
		}
	};

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setAppName("FindRDDMultipllication").setMaster("local");
		context = new JavaSparkContext(conf);

		ArrayList<Double> list1 = new ArrayList<>();
		ArrayList<Double> list2 = new ArrayList<>();

		list1.add(1.0);
		list1.add(2.3);
		list2.add(2.0);
		list2.add(3.5);

		JavaDoubleRDD rdd1 = context.parallelizeDoubles(list1);
		JavaDoubleRDD rdd2 = context.parallelizeDoubles(list2);

		JavaPairRDD<Integer, Double> pairs1 = rdd1.mapToPair(DOUBLE_VALUES);

		JavaPairRDD<Integer, Double> pairs2 = rdd2.mapToPair(DOUBLE_VALUES);
		
		JavaRDD<Tuple2<Double, Double>> pairs3 = pairs1.join(pairs2).values();
		JavaRDD<Double> results = pairs3.flatMap(RESULT);

		List<Double> finalList = results.collect();
		for (Double d : finalList)
			System.out.println(d);

	}
}