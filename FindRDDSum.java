import java.util.ArrayList;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FindRDDSum {

	private static String AppName = "FindRDDSquare";
	private static String Master = "local";

	public static JavaSparkContext SparkConfigure(String AppName, String Master) {

		SparkConf conf = new SparkConf().setAppName(AppName).setMaster(Master);

		JavaSparkContext context = new JavaSparkContext(conf);

		return context;
	}

	public Integer findSum(ArrayList<Integer> list) {

		JavaRDD<Integer> listInteger = FindRDDSum.SparkConfigure(AppName, Master).parallelize(list);

		int sum = listInteger.reduce((x, y) -> (x + y));

		return sum;
	}

	public static void main(String[] args) {

		ArrayList<Integer> list = new ArrayList<>();

		Random random = new Random();

		for (int i = 0; i < 1000; i++) {
			list.add(random.nextInt(1000));
		}

		FindRDDSum findRDDSum = new FindRDDSum();

		int result = findRDDSum.findSum(list);

		System.out.println("Sum: " + result);
	}
}