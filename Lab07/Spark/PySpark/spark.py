#load example file
hadoop_rdd = sc.textFile('hadoop_1m.txt')

#count number of like and display first 5 lines
hadoop_rdd.count()
hadoop_rdd.take(5)
#hadoop_rdd.collect()

#count email domains
#compare to Hadoop code from earlier lectures
domain_counts = (hadoop_rdd
                 .filter(lambda line: line.find("From:") == 0)
                 .map(lambda line: (line[line.find("@")+1:line.find(">")],1))
                 .reduceByKey(lambda a, b: a+b))

domain_counts.count()
domain_counts.take(5)

#Spark’s regular reduce does not group data by key as it is standard in Hadoop.
domain_counts_r = domain_counts.map(lambda a: 1).reduce(lambda a, b: a+b)
domain_counts_r

domain_endings_map = (hadoop_rdd
                      .filter(lambda line: line.find("From:") == 0)
                      .map(lambda line: line[line.find("@")+1:line.find(">")].split(".")[1:]))
domain_endings_map.count()
domain_endings_map.take(5)

domain_endings_flat = (hadoop_rdd
                      .filter(lambda line: line.find("From:") == 0)
                      .flatMap(lambda line: line[line.find("@")+1:line.find(">")].split(".")[1:]))
domain_endings_flat.count()
domain_endings_flat.take(5)


#Load library and data for SPARK SQL
from pyspark.sql import SQLContext, Row
sqlContext = SQLContext(sc)
hadoop_csv_rdd = sc.textFile('hadoop_1m.csv')

emails = (hadoop_csv_rdd
         .map(lambda l: l.split(","))
         .map(lambda p: Row(id=p[0], list=p[1], date1=p[2], date2=p[3], email=p[4], subject=p[5])))

emails.take(5)

schemaEmails = sqlContext.createDataFrame(emails)
schemaEmails.registerTempTable("emails")

#run sample query
lists = sqlContext.sql("SELECT COUNT(DISTINCT(email)) FROM emails")
lists.collect()
