DataFlow Pipeline to ingest data from bigquery apply dlp and insert into bigquery
=================================================================================

Command to stage template:
==========================
mvn compile exec:java -Dexec.mainClass=com.impetus.dataflow.IngestionPipeline -Dexec.args="--runner=DataflowRunner --project=learning-313419 --stagingLocation=gs://dataset2023/temp/staging --templateLocation=gs://dataset2023/IngestionPipeline --region=us-central1" -P dataflow-runner

Command to run the pipeline from local:
=======================================
mvn clean compile exec:java -Dexec.mainClass=com.impetus.dataflow.IngestionPipeline  -Dexec.args="--project=learning-313419 --gcpTempLocation=gs://dataset2023/temp/ --tempLocation=gs://dataset2023/temp/ --region=us-central1 --projectId=learning-313419 --deidentifyTemplateName=projects/learning-313419/locations/global/deidentifyTemplates/Copy-of-De-IdentiyTemplate1 --outputTableSpec=learning-313419:newoutput.test123 --query='select cid,bca from learning-313419.input_dataset.charge_volume limit 40' --runner=DataflowRunner"


