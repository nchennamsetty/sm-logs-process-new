

# Command to submit spark job
Use sbt-assembly "assembly" task to generate the uber JAR "./target/scala-2.10/sm-logs-process-assembly-1.0.jar" and then from edge node run:

spark-submit --class org.gogoair.info.SMLogProcessor  ~/narench/sm-logs-process-assembly-1.0.jar  "hdfs://til-ph-gbd-01.aircell.prod:8020/data/canonical/year=2016/month=3/day=29/source=sm_sla/"      "hdfs://til-ph-gbd-01.aircell.prod:8020/user/nchennamsetty/out32/" --master yarn-client --driver-memory 5g   --conf spark.executor.memory=5g  --conf spark.shuffle.file.buffer=100m  --conf spark.yarn.executor.memoryOverhead=750


# Using oozie
Copy workflow.xml and sm-logs-process-assembly-1.0.jar to HDFS and edit job.properties to reflect the HDFS paths. Then use
oozie job -config job.properties -run