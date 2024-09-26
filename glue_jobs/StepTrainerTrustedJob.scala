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
    // Script generated for node customer curated
    val customercurated_node1707946204192 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"multiLine": "false"}"""), connectionType="s3", format="json", options=JsonOptions("""{"paths": ["s3://sparkify-bucket-gin/customer/curated/"], "recurse": true}"""), transformationContext="customercurated_node1707946204192").getDynamicFrame()

    // Script generated for node step trainer landing
    val steptrainerlanding_node1707180956057 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"multiLine": "false"}"""), connectionType="s3", format="json", options=JsonOptions("""{"paths": ["s3://sparkify-bucket-gin/step_trainer/landing/"], "recurse": true}"""), transformationContext="steptrainerlanding_node1707180956057").getDynamicFrame()

    // Script generated for node SQL Query
    val SqlQuery694: String = """select * from step_trainer_landing
    join customer_curated
    on step_trainer_landing.serialnumber = customer_curated.serialnumber;""".stripMargin

    val SQLQuery_node1708118314581 = SparkSqlQuery.execute(glueContext = glueContext, sqlContext = sql, query = SqlQuery694, mapping = Map("customer_curated" -> customercurated_node1707946204192, "step_trainer_landing" -> steptrainerlanding_node1707180956057)
    )

    // Script generated for node Drop Fields
    val DropFields_node1700530620637 = SQLQuery_node1708118314581.dropFields(fieldNames=Seq("email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "customername"), transformationContext="DropFields_node1700530620637")

    // Script generated for node step trainer trusted
    val steptrainertrusted_node1700529751867 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://sparkify-bucket-gin/step_trainer/trusted/", "partitionKeys": [], "enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), transformationContext="steptrainertrusted_node1700529751867", format="json")
    steptrainertrusted_node1700529751867.setCatalogInfo(catalogDatabase="sparkify", catalogTableName="step_trainer_trusted")
    steptrainertrusted_node1700529751867.writeDynamicFrame(DropFields_node1700530620637)
    Job.commit()
  }
}