Program 1:
Files:
Makefile
Vasanthi_Mudunuri_Program_1.cpp

Execution Command:
mapred pipes -D mapreduce.pipes.isjavarecordreader=true -D mapreduce.pipes.isjavarecordwriter=true -input /CS5433/PA2/movies.dat -output /vmudunu/program1 -program /vmudunu/Vasanthi_Mudunuri_Program_1

Program 2:
Files:
Vasanthi_Mudunuri_Program_2_Mapper.py
Vasanthi_Mudunuri_Program_2_Reducer.py
ReadSequenceFile.java

Execution Command:
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -D mapreduce.job.queuename=GradClass -files Vasanthi_Mudunuri_Program_2_Mapper.py,Vasanthi_Mudunuri_Program_2_Reducer.py -mapper Vasanthi_Mudunuri_Program_2_Mapper.py -reducer Vasanthi_Mudunuri_Program_2_Reducer.py -input /CS5433/PA2/ratings.dat -output /vmudunu/program2 -outputformat org.apache.hadoop.mapred.SequenceFileOutputFormat

To read the output sequence file:
hadoop jar Vasanthi_Mudunuri_PA3.jar ReadSequenceFile /vmudunu/program2/part-00000

Program 3:
Vasanthi_Mudunuri_Program_3.java
ReadSequenceFile.java

Execution Command:
hadoop jar Vasanthi_Mudunuri_PA3.jar Vasanthi_Mudunuri_Program_3 /CS5433/PA3/CFS/CFS_2012_00A01_with_ann.csv /vmudunu/program3

To read the block level compressed file:
hdfs dfs -text /vmudunu/program3/part-r-00000
(or)
hadoop jar Vasanthi_Mudunuri_PA3.jar ReadSequenceFile /vmudunu/program3/part-r-00000

Program 4:
Vasanthi_Mudunuri_Program_4.java
TextPair.java
ReadSequenceFile.java

Execution Command:
hadoop jar Vasanthi_Mudunuri_PA3.jar Vasanthi_Mudunuri_Program_4 /CS5433/PA3/ACS/ACS_15_5YR_S0101_with_ann.csv /CS5433/PA3/CFS/CFS_2012_00A01_with_ann.csv /vmudunu/program4

Output Directory:
/vmudunu/program4/part-r-00000
Files in the Output Directory:
/vmudunu/program4/part-r-00000/data and /vmudunu/program4/part-r-00000/index
To read the output map file:
hadoop jar Vasanthi_Mudunuri_PA3.jar ReadSequenceFile /vmudunu/program4/part-r-00000/data

Program 5:
Vasanthi_Mudunuri_Program_5_Mapper.py
Vasanthi_Mudunuri_Program_5_Reducer.py

Execution Command:
hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar -D mapreduce.job.queuename=GradClass -D stream.num.map.output.key.fields=2 -D mapreduce.map.output.key.field.separator=. -D mapreduce.partition.keypartitioner.options=-k1,1 -D mapreduce.job.output.key.comparator.class=org.apache.hadoop.mapred.lib.KeyFieldBasedComparator -D mapreduce.partition.keycomparator.options='-k1r -k2nr' -files /home/vmudunu/movies.dat,Vasanthi_Mudunuri_Program_5_Mapper.py,Vasanthi_Mudunuri_Program_5_Reducer.py -mapper Vasanthi_Mudunuri_Program_5_Mapper.py -reducer Vasanthi_Mudunuri_Program_5_Reducer.py -input /CS5433/PA2/ratings.dat -output /vmudunu/program5 -partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner
