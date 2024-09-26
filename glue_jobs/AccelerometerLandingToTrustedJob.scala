import com.amazonaws.services.glue.GlueContext
import com.amazonaws.services.glue.MappingSpec
import com.amazonaws.services.glue.errors.CallSite
import com.amazonaws.services.glue.util.GlueArgParser
import com.amazonaws.services.glue.util.Job
import com.amazonaws.services.glue.util.JsonOptions
import org.apache.spark.SparkContext
import scala.collection.JavaConverters._

object GlueApp {
  def main(sysArgs: Array[String]) {
    val spark: SparkContext = new SparkContext()
    val glueContext: GlueContext = new GlueContext(spark)
    // @params: [JOB_NAME]
    val args = GlueArgParser.getResolvedOptions(sysArgs, Seq("JOB_NAME").toArray)
    Job.init(args("JOB_NAME"), glueContext, args.asJava)
    // Script generated for node accelerometer landing zone
    val accelerometerlandingzone_node1700529679236 = glueContext.getCatalogSource(database="stedi", tableName="accelerometer_landing", transformationContext="accelerometerlandingzone_node1700529679236").getDynamicFrame()

    // Script generated for node customer landing zone
    val customerlandingzone_node1700529677557 = glueContext.getCatalogSource(database="stedi", tableName="customer_trusted", transformationContext="customerlandingzone_node1700529677557").getDynamicFrame()

    // Script generated for node Join Customer
    val JoinCustomer_node1700529696052 = accelerometerlandingzone_node1700529679236.join(keys1=Seq("user"), keys2=Seq("email"), frame2=customerlandingzone_node1700529677557, transformationContext="JoinCustomer_node1700529696052")

    // Script generated for node Drop Fields
    val DropFields_node1700530620637 = JoinCustomer_node1700529696052.dropFields(fieldNames=Seq("email", "phone"), transformationContext="DropFields_node1700530620637")

    // Script generated for node accelerometer trusted zone
    val accelerometertrustedzone_node1700529751867 = glueContext.getSinkWithFormat(connectionType="s3", options=JsonOptions("""{"path": "s3://stedi-data-lakehouse/accelerometer/trusted/", "partitionKeys": [], "enableUpdateCatalog": true, "updateBehavior": "UPDATE_IN_DATABASE"}"""), transformationContext="accelerometertrustedzone_node1700529751867", format="json")
    accelerometertrustedzone_node1700529751867.setCatalogInfo(catalogDatabase="stedi", catalogTableName="accelerometer_trusted")
    accelerometertrustedzone_node1700529751867.writeDynamicFrame(DropFields_node1700530620637)
    Job.commit()
  }
}