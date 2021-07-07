import net.jpountz.util.Utils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.StandardScaler;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.*;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import scala.Int;

import java.util.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.split;

public class WuzzufJobsDAO {

    public static <val> void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.ERROR);


        SparkSession session = SparkSession.builder().appName("WuzzufJobs").master("local[1]").getOrCreate();
        DataFrameReader dataFrameReader = session.read();

        Dataset<Row> data = dataFrameReader.option("header", "true").csv("D:\\My-Github\\MachineLearningProject-By-Java\\Wuzzuf Data Project Spark Version\\src\\main\\resources\\Wuzzuf_Jobs.csv");

        // structure of the data
        System.out.println("=== Print out schema ===");
        data.printSchema();
        // summary of the data
        System.out.println("=== Print out summary ===");
        data.describe().show();

        // showing first 10 records of the data in table
        System.out.println("=== Print 20 records of responses table ===");
        data.show(10);

        // removing duplicates and nulls from the data
        System.out.println("Number of rows before cleaning : " + data.count());
        data.dropDuplicates();
        // there is nulls in Years Exp feature
        data = data.filter(row -> !row.get(5).equals("null Yrs of Exp"));
        System.out.println("Number of rows after cleaning : " + data.count());


        // count jobs for each country and displaying the result in pie chart
        RelationalGroupedDataset groupeddatabycompany = data.groupBy("Company");
        groupeddatabycompany.count().orderBy(col("count").desc()).show();
        Dataset<Row> top_10_Companies = groupeddatabycompany.count().orderBy(col("count").desc()).limit(10);

        // pie chart
        List<String> companies = top_10_Companies.select("Company").as(Encoders.STRING()).collectAsList();
        List<String> counts = top_10_Companies.select("count").as(Encoders.STRING()).collectAsList();
        // shoe=wing the chart
        PieChart compniesPieChart = pieChart.getPieChartforCompany(companies, counts);
        new SwingWrapper<>(compniesPieChart).displayChart();


       // the most popular job titles
         RelationalGroupedDataset groupeddatabyTitle = data.groupBy("Title");
         Dataset<Row> top_10_Titles = groupeddatabyTitle.count().orderBy(col("Count").desc()).limit(10);
         System.out.println("=== Print out top 10 job titles in the data ===");
         top_10_Titles.show();

        // bar chart
        List<String> titles  = top_10_Titles.select("Title").as(Encoders.STRING()).collectAsList();
        List<Long> titles_Counts = top_10_Titles.select("count").as(Encoders.LONG()).collectAsList();

        CategoryChart titlesBarChart = CategoryBarChart.barChart("Top 10 Titles", "Titles",titles, titles_Counts);
        new SwingWrapper<>(titlesBarChart).displayChart();

        // most popular locations(areas)
        RelationalGroupedDataset groupeddatabyLocation = data.groupBy("Location");
        Dataset<Row> top_10_Locations = groupeddatabyLocation.count().orderBy(col("count").desc()).limit(10);
        top_10_Locations.show();
        // showing top 10 locations in bar chart
        List<String> locations  = top_10_Locations.select("Location").as(Encoders.STRING()).collectAsList();
        List<Long> locations_Counts = top_10_Locations.select("count").as(Encoders.LONG()).collectAsList();

        CategoryChart locationsBarChart = CategoryBarChart.barChart("Top 10 Locations", "Locations", locations, locations_Counts);
        new SwingWrapper<>(locationsBarChart).displayChart();


        // skills


        List<String> skills  = data.select("Skills").as(Encoders.STRING()).collectAsList();
        List<String> sk = new ArrayList<>();
        for (String ls : skills) {
            String[] x = ls.split(",");
            for (String s : x) {
                sk.add(s);
            }}
        System.out.println(sk.size());
        System.out.println("=== The skills and the count of each skill ===");
        Map<String, Long> title_counts =
                sk.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

        title_counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(x -> System.out.println(x));

        System.out.println("=== The most important skills requiread ===");

        title_counts.entrySet().stream()
                .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).
                filter(x -> x.getValue() > 300).
                forEach(x -> System.out.println(x.getKey()));




        // another method to count skills
/*
        Dataset<Row> lss = data.select("Skills");

        JavaRDD<String> ss =  lss.toJavaRDD().flatMap(line -> Arrays.stream(line.toString().split(",")).iterator());

        Map<String, Long> wordCounts = ss.countByValue();
        System.out.println("=== The skills and the count of each skill ===");
        wordCounts.entrySet().stream()
                .filter(x -> x.getValue()>=1).forEach(x -> System.out.println(x.getKey() + " : " + x.getValue()));


        System.out.println("=== The most important skills requiread ===");
        wordCounts.entrySet().stream()
                .filter(x -> x.getValue()>300).forEach(x -> System.out.println(x.getKey()));
*/

        //copy of data
        Dataset<Row> df = data.as("df");

        // Factorize Years Exp feature in the original data
        StringIndexer idx = new StringIndexer();
        idx.setInputCol("YearsExp").setOutputCol("YearsExp indexed");
        data = idx.fit(data).transform(data);
        data.show(5);

        //preparing the copy of data for kmeans clustering

        // list of feature names and endexed feature names
        String columns[] = {"Title", "Company", "Location", "Type", "Level", "YearsExp", "Country"};
        String indexedColumns[] = {"Title indexed", "Company indexed", "Location indexed", "Type indexed", "Level indexed","YearsExp indexed", "Country indexed"};
        // Factorizing All Categorical Features
        int i ;
        for(i = 0; i < columns.length; i++){

        StringIndexer indexer = new StringIndexer();
        indexer.setInputCol(columns[i]).setOutputCol(indexedColumns[i]);
        df = indexer.fit(df).transform(df);
}

        // Casting The New Features To Double
        for(i = 0; i < columns.length; i++){
            df = df.withColumn (indexedColumns[i], df.col (indexedColumns[i]).cast ("double"));
        }
        // showing df after preparation
        df.show(5);

        // showing new schema to check types of new features
        System.out.println("=== Print out the new schema ===");
        df.printSchema();


        // vector assembler that will contain feature columns
    //    String inputColumns[] = {"Title indexed", "Company indexed", "Location indexed", "Type indexed", "Level indexed","YearsExp indexed", "Country indexed", "Skills indexed"};
        VectorAssembler vectorAssembler = new VectorAssembler ();
        vectorAssembler.setInputCols (indexedColumns).setOutputCol("features");
        Dataset<Row> trainData = vectorAssembler.transform (df);
        // show some data after transforming
        System.out.println("=== the train data which are ready for the model ===");
        trainData.show(10);

        // Trains a k-means model with k = 6
        KMeans kmeans4 = new KMeans().setK(6).setSeed(1L);
        kmeans4.setFeaturesCol("features");
        KMeansModel model4 = kmeans4.fit(trainData);
        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        double WSSE = model4.computeCost(trainData);
        System.out.println("Within Set Sum of Squared Errors of k = 6 = " + WSSE);

        // Trains a k-means model with k = 7
        KMeans kmeans5 = new KMeans().setK(7).setSeed(1L);
        kmeans5.setFeaturesCol("features");
        KMeansModel model5 = kmeans5.fit(trainData);
        // Evaluate clustering by computing Within Set Sum of Squared Errors.
        double WSSSE = model5.computeCost(trainData);
        System.out.println("Within Set Sum of Squared Errors of k = 7 = " + WSSSE);
   }
}
