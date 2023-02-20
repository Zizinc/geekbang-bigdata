import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class Exercise {
    public static void runQuestions(Dataset<Row> df) {
        Dataset<Row> studentDf = df.select("班级", "姓名", "年龄", "性别")
                .distinct()
                .cache();

        System.out.println("2. 一共有多少个小于20岁的人参加考试？");
        System.out.println("3. 一共有多少个等于20岁的人参加考试？");
        System.out.println("4. 一共有多少个大于20岁的人参加考试？");
        studentDf.withColumn("age_group",
                        when(col("年龄").lt(20), "age < 20")
                        .when(col("年龄").equalTo(20), "age = 20")
                        .otherwise("age > 20"))
                .groupBy("age_group")
                .agg(count(lit(1)).as("count"))
                .show();

        System.out.println("5. 一共有多个男生参加考试？");
        System.out.println("6. 一共有多少个女生参加考试？");
        studentDf.groupBy("性别")
                .agg(count(lit(1)).as("count"))
                .show();

        System.out.println("7. 12班有多少人参加考试？");
        System.out.println("8. 13班有多少人参加考试？");
        studentDf.groupBy("班级")
                .agg(count(lit(1)).as("count"))
                .show();

        System.out.println("9. 语文科目的平均成绩是多少？");
        System.out.println("10. 数学科目的平均成绩是多少？");
        System.out.println("11. 英语科目的平均成绩是多少？");
        df.groupBy("科目")
                .agg(avg(col("成绩")).as("average_score"))
                .show();

        System.out.println("12. 每个人平均成绩是多少？");
        df.groupBy("姓名")
                .agg(avg(col("成绩")).as("average_score"))
                .show();

        System.out.println("13. 12班平均成绩是多少？");
        System.out.println("16. 13班平均成绩是多少？");
        df.groupBy("班级")
                .agg(avg(col("成绩")).as("average_score"))
                .show();

        System.out.println("14. 12班男生平均总成绩是多少？");
        System.out.println("15. 12班女生平均总成绩是多少？");
        System.out.println("17. 13班男生平均总成绩是多少？");
        System.out.println("18. 13班女生平均总成绩是多少？");
        df.groupBy("班级", "性别")
                .agg(avg(col("成绩")).as("average_score"))
                .show();

        System.out.println("19. 全校语文成绩最高分是多少？");
        df.filter(col("科目").equalTo("chinese"))
                .groupBy("科目")
                .agg(max(col("成绩")).as("max_score"))
                .show();

        System.out.println("20. 12班语文成绩最低分是多少？");
        df.filter(col("科目").equalTo("chinese").and(col("班级").equalTo("12")))
                .groupBy("班级", "科目")
                .agg(min(col("成绩")).as("min_score"))
                .show();

        System.out.println("21. 13班数学最高成绩是多少？");
        df.filter(col("科目").equalTo("math").and(col("班级").equalTo("13")))
                .groupBy("班级", "科目")
                .agg(max(col("成绩")).as("max_score"))
                .show();

        System.out.println("22. 总成绩大于150分的12班的女生有几个？");
        long a22 = df.filter(col("班级").equalTo("12").and(col("性别").equalTo("女")))
                .groupBy("班级", "性别")
                .agg(sum(col("成绩")).as("total_score"))
                .filter(col("total_score").gt(150))
                .count();
        System.out.println("22A. 总成绩大于150分的12班的女生有" + a22 + "个\n");

        System.out.println("23. 总成绩大于150分，且数学大于等于70，且年龄大于等于19岁的学生的平均成绩是多少？");
        Dataset<Row> mathGeq70AndAgeGeq19Df = df.filter(
                col("科目").equalTo("math")
                .and(col("成绩").geq(70))
                .and(col("年龄").geq(19)));
        Dataset<Row> totalScoreGt150Df = df.groupBy(col("姓名"))
                .agg(sum(col("成绩")).as("total_score"), avg(col("成绩")).as("average_score"))
                .where(col("total_score").gt(150));
        mathGeq70AndAgeGeq19Df.as("a").join(totalScoreGt150Df.as("b")
                , col("a.姓名").equalTo(col("b.姓名")))
                //.select(col("b.姓名"), col("b.average_score"))
                .show();
    }

    public static void main(String[] args) {
        Logger.getLogger("org").setLevel(Level.WARN);
        Logger.getLogger("akka").setLevel(Level.WARN);

        SparkSession spark = SparkSession
                .builder()
                .appName("Exercise App")
                .master("local[*]")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("sep", " ")
                .csv("src/main/resources/test.txt");

        Dataset<Row> formattedDf = df.withColumn("班级", col("班级").cast(StringType));
        formattedDf.show(false);

        runQuestions(formattedDf);
    }
}
