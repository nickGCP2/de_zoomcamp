mkdir spark 

JAVA 
wget https://download.java.net/java/GA/jdk11/13/GPL/openjdk-11.0.1_linux-x64_bin.tar.gz
tar xzvf openjdk-11.0.1_linux-x64_bin.tar.gz
rm openjdk-11.0.1_linux-x64_bin.tar.gz
nano .bashrc
export JAVA_HOME="${HOME}/spark/jdk-11.0.1"
export PATH="${JAVA_HOME}/bin:${PATH}"

SPARK
wget https://dlcdn.apache.org/spark/spark-3.0.3/spark-3.0.3-bin-hadoop3.2.tgz
tar xzvf spark-3.0.3-bin-hadoop3.2.tgz
rm spark-3.0.3-bin-hadoop3.2.tgz
nano .bashrc
export SPARK_HOME="${HOME}/spark/spark-3.0.3-bin-hadoop3.2"
export PATH="${SPARK_HOME}/bin:${PATH}"