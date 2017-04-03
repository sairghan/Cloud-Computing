Readme file:

Note: Jar file is located in the zip folder.Location: Access1\target\Access1-0.0.1-SNAPSHOT.jar

1- To run spark shell

spark/bin/spark-shell --master yarn-client
 
2- To run task 2

spark/bin/spark-submit \
--class pitt.cloud.Access1.Task2 \
--master yarn \
--deploy-mode client \
--driver-memory 512m --executor-memory 512m \
--executor-cores 1 \
--queue default \
/root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar
 
3- To run task 3
	a- to run 1st question
spark/bin/spark-submit \
  --master yarn \
  --class pitt.cloud.Access1.AcessLog1 \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --queue default \
  /root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar
  
  b- to run 2nd question
spark/bin/spark-submit \
  --master yarn \
  --class pitt.cloud.Access1.AccessLog2 \
  --driver-memory 512m \
  --executor-memory 512m \
  --executor-cores 1 \
  --queue default \
  /root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar
  
  c- to run 3rd question
spark/bin/spark-submit \
--class pitt.cloud.Access1.Accesslog3 \
--master yarn \
--deploy-mode client \
--driver-memory 512m \     
--executor-memory 512m \
--executor-cores 1 \
--queue default \
/root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar

	d- to run 4th question
spark/bin/spark-submit \
--class pitt.cloud.Access1.Accesslog4 \
--master yarn \
--deploy-mode client \
--driver-memory 512m --executor-memory 512m \
--executor-cores 1 \
--queue default \
/root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar


	e-to run task 3, part2(load file once)
spark/bin/spark-submit \
--class pitt.cloud.Access1.Accesspart2 \
--master yarn \
--deploy-mode client \
--driver-memory 512m --executor-memory 512m \
--executor-cores 1 \
--queue default \
/root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar
	
	f-to run task 3, part2(load file everytime)
spark/bin/spark-submit \
--class pitt.cloud.Access1.Accesspart22 \
--master yarn \
--deploy-mode client \
--driver-memory 512m --executor-memory 512m \
--executor-cores 1 \
--queue default \
/root/Accesslogproj2/Access1-0.0.1-SNAPSHOT.jar

