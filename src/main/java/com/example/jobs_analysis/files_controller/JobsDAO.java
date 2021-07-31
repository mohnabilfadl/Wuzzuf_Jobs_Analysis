package com.example.jobs_analysis.files_controller;



import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructType;
import org.knowm.xchart.*;
import org.knowm.xchart.style.Styler;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


import static org.apache.spark.sql.functions.*;


public class JobsDAO {
      
        // Create Spark Session to create connection to Spark
        public DataFrameReader  getFrameReader() {
            final SparkSession session  = SparkSession.builder().appName("Jobs").master("local[2]").getOrCreate();
            return session.read();
        }
        public SparkSession getFrameRead(){
            final SparkSession session = SparkSession.builder().appName("Jobs").master("local[2]").getOrCreate();
            return session;
        }


        // read data , clean null and duplicate values
        public Dataset<Row> getDataset() {
            Dataset<Row> data = getFrameReader().option("header", "true").csv("src/main/resources/files/Wuzzuf_Jobs.csv");
            data = data.na().drop().distinct();
            return data;
        }

        Dataset<Row> data = getDataset();

        // show some of the data
        public String ShowData(){
            List<Row> first_20_records = data.limit(20).collectAsList();
            return DisplayHtml.displayrows(data.columns(), first_20_records);
        }


        // Print Schema to see column names, types and other metadata
        public String structure(){
            StructType d = data.schema();
            return d.prettyJson();
        }



        // Print summary
        public String summary() {
            Dataset<Row> d = data.summary();
            List<Row> summary = d.collectAsList();
            return DisplayHtml.displayrows(d.columns(), summary);
        }


        // Count the jobs for each company and display that in order
        public String jobsByCompany(){
            Dataset<Row> company = data.groupBy("Company").count().orderBy(col("count").desc()).limit(20);
            List<Row> top_Companies = company.collectAsList();
            return DisplayHtml.displayrows(company.columns(), top_Companies);
        }


        //Show  previous data in a pie chart
        public String pieChartForCompany() throws IOException {
            Dataset<Row> company = data.groupBy("Company").count().orderBy(col("count").desc()).limit(10);
            List<String> companies = company.select("Company").as(Encoders.STRING()).collectAsList();
            List<String> counts = company.select("count").as(Encoders.STRING()).collectAsList();

            PieChart chart = new PieChartBuilder().width(800).height(600).title("Most Demanding Companies For Jobs").build();
            for (int i = 0; i < 10; i++) {
                chart.addSeries(companies.get(i), Integer.parseInt(counts.get(i)));
            }
            String path = "src/main/resources/files/pieChart.png";
            return DisplayHtml.viewchart(path);
        }

        //Most popular job titles
        public String JobsByTitles(){
            Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc()).limit(20);
            List<Row> top_titles = title.collectAsList();
            return DisplayHtml.displayrows(title.columns(), top_titles);
        }

        // Bar chart of the previous data
        public String TitlesBarChart() throws IOException {
            Dataset<Row> title = data.groupBy("Title").count().orderBy(col("count").desc()).limit(10);
            List<String> titles = title .select("Title").as(Encoders.STRING()).collectAsList();
            List<Long> counts = title .select("count").as(Encoders.LONG()).collectAsList();

            CategoryChart bar = new CategoryChartBuilder().width(800).height(600).title("Most popular job titles").xAxisTitle("Title").yAxisTitle("Jobs").build();
            bar.getStyler().setHasAnnotations(true);
            bar.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
            bar.getStyler().setStacked(true);
            bar.getStyler().setXAxisLabelRotation(45);
            bar.addSeries("Jobs per title", titles, counts);

            String path = "src/main/resources/files/barChart1.png";
            return DisplayHtml.viewchart(path);
        }

        // Most popular areas
        public String JobsByAreas(){
            Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc()).limit(20);;
            List<Row> top_titles = area.collectAsList();
            return DisplayHtml.displayrows(area.columns(), top_titles);
        }

        // Bar chart of the previous data
        public String areasBarChart() throws IOException {
            Dataset<Row> area = data.groupBy("Location").count().orderBy(col("count").desc()).limit(10);
            List<String> location = area.select("Location").as(Encoders.STRING()).collectAsList();
            List<Long> counts = area.select("count").as(Encoders.LONG()).collectAsList();

            CategoryChart bar2 = new CategoryChartBuilder().width(800).height(600).title("Most popular job locations").xAxisTitle("Location").yAxisTitle("Jobs").build();
            bar2.getStyler().setHasAnnotations(true);
            bar2.getStyler().setLegendPosition(Styler.LegendPosition.InsideNE);
            bar2.getStyler().setStacked(true);
            bar2.getStyler().setXAxisLabelRotation(45);
            bar2.addSeries("Jobs per location", location, counts);

            String path = "src/main/resources/files/barChart2.png";
            return DisplayHtml.viewchart(path);

        }


        // Skills one by one
        public ResponseEntity<Object> skill() {
            List<String> allSkills = data.select("Skills").as(Encoders.STRING()).collectAsList();
            List<String> skills = new ArrayList<>();
            for (String ls : allSkills) {
                String[] x = ls.split(",");
                for (String s : x) {
                    skills.add(s);
                }
            }

            Map<String, Long> skill_counts =
                    skills.stream().collect(Collectors.groupingBy(e -> e, Collectors.counting()));

            List<String> s = new ArrayList<>();
            List<Long> counts = new ArrayList<>();
            for (Map.Entry<String, Long> m : skill_counts.entrySet()) {
                s.add(m.getKey());
                counts.add(m.getValue());
            }


            return new ResponseEntity<>(skill_counts.entrySet().stream()
                    .sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).filter(x -> x.getValue() > 100), HttpStatus.OK);

        }


        // Factorize Years Exp feature in the original data
        public String factYearsExp(){
            StringIndexer idx = new StringIndexer();
            idx.setInputCol("YearsExp").setOutputCol("YearsExp indexed");
            Dataset<Row> new_data = idx.fit(data).transform(data);
            String columns[] = {"YearsExp", "YearsExp indexed"};
            List<Row> yeasExpIndexed = new_data.select("YearsExp", "YearsExp indexed").limit(30).collectAsList();
            return DisplayHtml.displayrows(columns, yeasExpIndexed);
        }
}

