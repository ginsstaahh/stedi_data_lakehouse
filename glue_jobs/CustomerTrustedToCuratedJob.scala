import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
import org.apache.spark.sql.functions._
import com.amazonaws.services.glue.DynamicFrame
import org.apache.spark.sql.SQLContext
import com.amazonaws.services.glue.errors.CallSite

object SparkSqlQuery {
  def execute(glueContext: GlueContext, sqlContext: SQLContext, query: String, mapping: Map[String, DynamicFrame]) : DynamicFrame = {
    for ((alias, frame) <- mapping) {
      frame.toDF().createOrReplaceTempView(alias)
    }
    val resultDataFrame = sqlContext.sql(query)
    return DynamicFrame(resultDataFrame, glueContext)
  }
}

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    val sql: SQLContext = new SQLContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    // Script generated for node accelerometer landing zone
    val accelerometerlandingzone_node1700529679236 = glueContext.getCatalogSource(database="stedi", tableName="accelerometer_landing", transformationContext="accelerometerlandingzone_node1700529679236").getDynamicFrame()

    // Script generated for node customer trusted
    val customertrusted_node1700529677557 = glueContext.getCatalogSource(database="stedi", tableName="customer_trusted", transformationContext="customertrusted_node1700529677557").getDynamicFrame()

    // Script generated for node SQL Query
    val SqlQuery2717: String = """select customername, email, phone, birthday, serialnumber, registrationdate, lastupdatedate, sharewithresearchasofdate, sharewithpublicasofdate
    from customer
    join accelerometer
    on accelerometer.user = customer.email""".stripMargin

    val SQLQuery_node1708714281489 = SparkSqlQuery.execute(glueContext = glueContext, sqlContext = sql, query = SqlQuery2717, mapping = Map("accelerometer" -> accelerometerlandingzone_node1700529679236, "customer" -> customertrusted_node1700529677557)
    )

    // Script generated for node Drop Duplicates
    val DropDuplicates_node1708714184661 = DynamicFrame(SQLQuery_node1708714281489.toDF().dropDuplicates(), glueContext)

    // Script generated for node customer curated
    val customercurated_node1708717547965 = glueContext.getCatalogSink(database="stedi", tableName="customer_curated",additionalOptions=JsonOptions("""{"enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), transformationContext="customercurated_node1708717547965").writeDynamicFrame(DropDuplicates_node1708714184661)

    Job.commit()
  }
}