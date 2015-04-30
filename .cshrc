setenv JAVA_HOME /usr
setenv HADOOP_HOME /localhome/hadoop/hadoop-2.6.0
setenv SPARK_HOME /localhome/hadoop/spark-1.2.1-bin-hadoop2.4
#export HADOOP_PREFIX=${HADOOP_HOME}
#export HADOOP_MAPRED_HOME=${HADOOP_HOME}
#export HADOOP_COMMON_HOME=${HADOOP_HOME}
#export HADOOP_HDFS_HOME=${HADOOP_HOME}
#export HADOOP_YARN_HOME=${HADOOP_HOME}
#export YARN_HOME=${HADOOP_HOME}
#export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_PREFIX}/lib/native
#export HADOOP_OPTS="-Djava.library.path=$HADOOP_PREFIX/lib"
setenv HADOOP_CONF_DIR ${HADOOP_HOME}/etc/hadoop
setenv YARN_CONF_DIR ${HADOOP_HOME}/etc/hadoop
setenv PATH ${PATH}:${HADOOP_HOME}/bin:${SPARK_HOME}/bin:/bin:/usr/bin
setenv HADOOP_CLASSPATH $(hadoop classpath)

setenv SPARK_JAR ${SPARK_HOME}/lib/spark-assembly-1.2.1-hadoop2.4.0.jar
setenv SPARK_YARN true
setenv SPARK_HADOOP_VERSION 2.6.0
setenv SPARK_YARN_USER_ENV "CLASSPATH=${YARN_CONF_DIR}"
setenv SPARK_PRINT_LAUNCH_COMMAND 1

