import java.util.ArrayList;
import java.util.Random;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FindRDDSquare {

	private static String AppName = "FindRDDSquare";
	private static String Master = "local";

	public static JavaSparkContext SparkConfigure(String AppName, String Master) {

		SparkConf conf = new SparkConf().setAppName(AppName).setMaster(Master);

		JavaSparkContext context = new JavaSparkContext(conf);

		return context;
	}

	public JavaDoubleRDD findSquare(ArrayList<Integer> list) {

		JavaRDD<Integer> listInteger = FindRDDSquare.SparkConfigure(AppName, Master).parallelize(list);

		JavaDoubleRDD list1Square = listInteger.mapToDouble((x) -> (x * x));

		return list1Square;
	}

	public static void main(String[] args) {

		ArrayList<Integer> list = new ArrayList<>();

		Random random = new Random();

		for (int i = 0; i < 10; i++) {
			list.add(random.nextInt(10));
		}

		FindRDDSquare findRDDSquare = new FindRDDSquare();

		JavaDoubleRDD resultList = findRDDSquare.findSquare(list);

		resultList.collect().forEach(System.out::println);
	}
}