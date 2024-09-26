import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._
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
    // Script generated for node Customer Landing
    val CustomerLanding_node1700090684865 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"multiline": false}"""), connectionType="s3", format="json", options=JsonOptions("""{"paths": ["s3://sparkify-bucket-gin/customer/landing/"], "recurse": true}"""), transformationContext="CustomerLanding_node1700090684865").getDynamicFrame()

    // Script generated for node SQL Query
    val SqlQuery3171: String = """select * from customer_landing
    where customer_landing.shareWithResearchAsOfDate != 0;""".stripMargin

    val SQLQuery_node1707166424088 = SparkSqlQuery.execute(glueContext = glueContext, sqlContext = sql, query = SqlQuery3171, mapping = Map("customer_landing" -> CustomerLanding_node1700090684865)
    )

    // Script generated for node Customer Trusted
    val CustomerTrusted_node1700090688826 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://sparkify-bucket-gin/customer/trusted/", "partitionKeys": [], "enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), transformationContext="CustomerTrusted_node1700090688826", format="json")
    CustomerTrusted_node1700090688826.setCatalogInfo(catalogDatabase="sparkify", catalogTableName="customer_trusted")
    CustomerTrusted_node1700090688826.writeDynamicFrame(SQLQuery_node1707166424088)
    Job.commit()
  }
}