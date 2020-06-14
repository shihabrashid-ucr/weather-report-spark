import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.IOException;

public class WeatherReport {
    public static void main(String[] args) throws IOException {
        long time = System.nanoTime();
        SparkSession session = SparkSession.builder().appName("CS236 Project").master("local").getOrCreate();

        Dataset<Row> weatherStation = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("WeatherStationLocations.csv");
        //FOR BONUS
        Dataset<Row> stateSpatial = session.read()
                .option("delimiter", ",")
                .option("header", "true")
                .option("inferSchema", "true")
                .csv("statelatlong.csv");

        stateSpatial.createOrReplaceTempView("stateSpatial_view");
        weatherStation.createOrReplaceTempView("weather_view");

        Dataset<Row> onlyUS = session.sql("SELECT * from weather_view WHERE CTRY='US' AND STATE IS NOT NULL");
        onlyUS.createOrReplaceTempView("onlyUS_view");
        Dataset<Row> noState = session.sql("select * from weather_view WHERE CTRY='US' AND STATE IS NULL AND LAT IS NOT NULL AND LON IS NOT NULL " +
                "AND LAT <> 0 AND LON <> 0");// AND BEGIN IS NOT NULL AND END IS NOT NULL");
        noState.createOrReplaceTempView("noState_view");

        Dataset<Row> calStateJoin = session.sql("select D1.state AS old_state, D2.State, D2.Latitude, D2.Longitude, D1.USAF, D1.WBAN, D1.`STATION NAME`, " +
                "D1.CTRY, D1.LAT, D1.LON, D1.`ELEV(M)`, D1.Begin, D1.End " +
                "From noState_view D1 CROSS JOIN stateSpatial_view D2 ");
        calStateJoin.createOrReplaceTempView("calStateJoin_view");
        //Condition to check spatial distance to match
        calStateJoin = calStateJoin.withColumn("old_state",
                functions.when(functions.abs(functions.col("Latitude").minus(functions.col("LAT"))).between(0, 1.5)
                                .and(functions.abs(functions.col("Longitude").minus(functions.col("LON"))).between(0, 4)),
                functions.col("State")));
        calStateJoin.createOrReplaceTempView("updated_view");

        Dataset<Row> noStateFinal = session.sql("select * from updated_view where old_state is not null");
        noStateFinal.createOrReplaceTempView("noStateFinal_view");
        //merge the two US lists (one with approximated state, one with state value not null)
        Dataset<Row> cleanedNoState = session.sql("select USAF, WBAN, `STATION NAME`, CTRY, old_state AS STATE, LAT, LON, `ELEV(M)`, BEGIN, END " +
                "FROM noStateFinal_view");
        Dataset<Row> finalUS = onlyUS.union(cleanedNoState);
        finalUS.createOrReplaceTempView("finalUS_view");
        //BONUS ENDS HERE


        Dataset<Row> recordings06Init = session.read()
                .text("2006.txt")
                .filter("value not like 'STN--%'");
        Dataset<Row> recordings07Init = session.read()
                .text("2007.txt")
                .filter("value not like 'STN--%'");
        Dataset<Row> recordings08Init = session.read()
                .text("2008.txt")
                .filter("value not like 'STN--%'");
        Dataset<Row> recordings09Init = session.read()
                .text("2009.txt")
                .filter("value not like 'STN--%'");

        Dataset<Row> combineRecordings = recordings06Init.union(recordings07Init).union(recordings08Init).union(recordings09Init);
        //combineRecordings.show(20);


        combineRecordings = combineRecordings.withColumn("value", functions.regexp_replace(functions.col("value"), " +", " "));
        combineRecordings = combineRecordings.withColumn("value", functions.split(functions.col("value"), " "));
        combineRecordings = combineRecordings.withColumn("USAF", functions.col("value").getItem(0));
        combineRecordings = combineRecordings.withColumn("WBAN", functions.col("value").getItem(1));
        combineRecordings = combineRecordings.withColumn("MONTH", functions.col("value").getItem(2).substr(5, 2));
        combineRecordings = combineRecordings.withColumn("PRCP", functions.col("value").getItem(19));
        combineRecordings = combineRecordings.drop("value").filter("PRCP NOT LIKE '99.99'");
        combineRecordings.createOrReplaceTempView("combineRecordings_view");

        Dataset<Row> joinedData = session.sql("Select D1.USAF, D1.CTRY, D1.STATE, D2.MONTH, D2.PRCP from onlyUS_view AS D1 inner join" +
                " combineRecordings_view AS D2 ON D1.USAF = D2.USAF"); //ORDER BY D2.PRCP DESC");

        joinedData = joinedData.withColumn("PRCP_UPDATED", functions.when(functions.col("PRCP").contains("A"), functions.col("PRCP").substr(0, 4).multiply(4))
        .when(functions.col("PRCP").contains("B"), functions.col("PRCP").substr(0, 4).multiply(2))
        .when(functions.col("PRCP").contains("C"), functions.col("PRCP").substr(0, 4).multiply(1.33))
        .when(functions.col("PRCP").contains("D"), functions.col("PRCP").substr(0, 4).multiply(1))
        .when(functions.col("PRCP").contains("E"), functions.col("PRCP").substr(0, 4).multiply(2))
        .when(functions.col("PRCP").contains("F"), functions.col("PRCP").substr(0, 4).multiply(1))
        .when(functions.col("PRCP").contains("G"), functions.col("PRCP").substr(0, 4).multiply(1))
        .when(functions.col("PRCP").contains("H"), functions.col("PRCP").substr(0, 4).multiply(0))
        .when(functions.col("PRCP").contains("I"), functions.col("PRCP").substr(0, 4).multiply(0))
        .otherwise(0));
        joinedData.createOrReplaceTempView("joined_view");

        //joinedData.show(15);

        Dataset<Row> groupMonth = session.sql("Select State, Month, Avg(PRCP_UPDATED) AS AVG_PRCP From joined_view GROUP BY Month, State ORDER BY State");
        groupMonth.createOrReplaceTempView("groupMonth_view");
      //  groupMonth.show(15);

        Dataset<Row> maxMonth = session.sql("SELECT t1.AVG_PRCP as MAX_PRCP,  t1.STATE, t1.MONTH as MAX_MONTH FROM groupMonth_view as t1 JOIN " +
                "(SELECT MAX(AVG_PRCP) as INNER_MAX, STATE FROM groupMonth_view GROUP BY STATE) t2 ON t1.STATE = t2.STATE  AND t1.AVG_PRCP = t2.INNER_MAX");
        //maxMonth.show(15);
        maxMonth.createOrReplaceTempView("maxMonth_view");

        Dataset<Row> minMonth = session.sql("SELECT t1.AVG_PRCP as MIN_PRCP,  t1.STATE, t1.MONTH as MIN_MONTH FROM groupMonth_view as t1 JOIN " +
                "(SELECT MIN(AVG_PRCP) as INNER_MIN, STATE FROM groupMonth_view GROUP BY STATE) t2 ON t1.STATE = t2.STATE  AND t1.AVG_PRCP = t2.INNER_MIN");
       // minMonth.show(15);
        minMonth.createOrReplaceTempView("minMonth_view");

        //joining maxMonth and minMonth table
        Dataset<Row> maxMin = session.sql("Select maxMonth_view.STATE, maxMonth_view.MAX_PRCP, maxMonth_view.MAX_MONTH, minMonth_view.MIN_PRCP, " +
                "minMonth_view.MIN_MONTH FROM maxMonth_view JOIN minMonth_view ON maxMonth_view.STATE = minMonth_view.STATE");
        maxMin.createOrReplaceTempView("maxMin_view");

        Dataset<Row> difference = session.sql("Select *, MAX_PRCP-MIN_PRCP as DIFFERENCE FROM maxMin_view WHERE (MAX_PRCP <> 0 AND MIN_PRCP <> 0) ORDER BY DIFFERENCE ASC");

        difference = difference.withColumn("MAX_MONTH", functions.when(functions.col("MAX_MONTH").equalTo("01"), "January")
                .when(functions.col("MAX_MONTH").equalTo("02"), "February")
                .when(functions.col("MAX_MONTH").equalTo("03"), "March")
                .when(functions.col("MAX_MONTH").equalTo("04"), "April")
                .when(functions.col("MAX_MONTH").equalTo("05"), "May")
                .when(functions.col("MAX_MONTH").equalTo("06"), "June")
                .when(functions.col("MAX_MONTH").equalTo("07"), "July")
                .when(functions.col("MAX_MONTH").equalTo("08"), "August")
                .when(functions.col("MAX_MONTH").equalTo("09"), "September")
                .when(functions.col("MAX_MONTH").equalTo("10"), "October")
                .when(functions.col("MAX_MONTH").equalTo("11"), "November")
                .when(functions.col("MAX_MONTH").equalTo("12"), "December"));
        difference = difference.withColumn("MIN_MONTH", functions.when(functions.col("MIN_MONTH").equalTo("01"), "January")
                .when(functions.col("MIN_MONTH").equalTo("02"), "February")
                .when(functions.col("MIN_MONTH").equalTo("03"), "March")
                .when(functions.col("MIN_MONTH").equalTo("04"), "April")
                .when(functions.col("MIN_MONTH").equalTo("05"), "May")
                .when(functions.col("MIN_MONTH").equalTo("06"), "June")
                .when(functions.col("MIN_MONTH").equalTo("07"), "July")
                .when(functions.col("MIN_MONTH").equalTo("08"), "August")
                .when(functions.col("MIN_MONTH").equalTo("09"), "September")
                .when(functions.col("MIN_MONTH").equalTo("10"), "October")
                .when(functions.col("MIN_MONTH").equalTo("11"), "November")
                .when(functions.col("MIN_MONTH").equalTo("12"), "December"));

        difference.coalesce(1).write().option("header", "true").option("delimiter", ",").mode("overwrite").csv("output");
        System.out.println("Total time to execute: " + (System.nanoTime() - time)/1000000000 + " seconds");

    }
}
