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
    // Script generated for node accelerometer trusted
    val accelerometertrusted_node1708119331302 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"multiline": false}"""), connectionType="s3", format="json", options=JsonOptions("""{"paths": ["s3://stedi-data-lakehouse/accelerometer/trusted/"], "recurse": true}"""), transformationContext="accelerometertrusted_node1708119331302").getDynamicFrame()

    // Script generated for node step trainer trusted
    val steptrainertrusted_node1707180956057 = glueContext.getSourceWithFormat(formatOptions=JsonOptions("""{"multiline": false}"""), connectionType="s3", format="json", options=JsonOptions("""{"paths": ["s3://stedi-data-lakehouse/step_trainer/trusted/"], "recurse": true}"""), transformationContext="steptrainertrusted_node1707180956057").getDynamicFrame()

    // Script generated for node SQL Query
    val SqlQuery2684: String = """select * from accelerometer
    join step_trainer
    on accelerometer.timestamp = step_trainer.sensorreadingtime;""".stripMargin

    val SQLQuery_node1708120300334 = SparkSqlQuery.execute(glueContext = glueContext, sqlContext = sql, query = SqlQuery2684, mapping = Map("accelerometer" -> accelerometertrusted_node1708119331302, "step_trainer" -> steptrainertrusted_node1707180956057)
    )

    // Script generated for node ML trusted
    val MLtrusted_node1700529751867 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://stedi-data-lakehouse/machine_learning/curated/", "partitionKeys": [], "enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), transformationContext="MLtrusted_node1700529751867", format="json")
    MLtrusted_node1700529751867.setCatalogInfo(catalogDatabase="stedi", catalogTableName="machine_learning_curated")
    MLtrusted_node1700529751867.writeDynamicFrame(SQLQuery_node1708120300334)
    Job.commit()
  }
}