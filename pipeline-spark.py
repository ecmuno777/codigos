'''
Neste exemplo, criaremos um pipeline de dados que realiza as seguintes etapas:

1 - Ler dados de livros de um Data Lake Hadoop. Nesse diretório hadoop temos diversos arquivos txt 
onde dentro dos arquivos estao os textos dos livros. o nome do arquivo sera na forma nomedolivro.txt
2 - Salvar os dados em uma primeira camada no HBase.
3 - Ler os dados do HBase e contar as palavras em cada livro.
4 - Salvar os resultados em uma tabela no PostgreSQL.

'''

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
import happybase
import os

# Configuração do Spark
conf = SparkConf().setAppName("BooksPipeline").setMaster("local")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

# Etapa 1: Ler dados de livros de um Data Lake Hadoop (HDFS) como arquivos de texto
hdfs_books_path = "hdfs://localhost:9000/path/to/books/*"
books_rdd = sc.wholeTextFiles(hdfs_books_path)

# Extrair o nome do livro do caminho do arquivo e remover a extensão .txt
books_rdd = books_rdd.map(lambda x: (os.path.splitext(os.path.basename(x[0]))[0], x[1]))
books = spark.createDataFrame(books_rdd, schema=["title", "content"])

# Etapa 2: Salvar os dados em uma primeira camada no HBase
hbase_connection = happybase.Connection("localhost")
hbase_table = hbase_connection.table("books")

# Converter o DataFrame do HDFS para um RDD e salvar no HBase
def put_row(row):
    hbase_table.put(row.title, {"content:data": row.content})

books.rdd.foreach(put_row)

# Etapa 3: Ler os dados do HBase
def get_books(scanner):
    for key, data in scanner:
        yield (key.decode("utf-8"), data[b"content:data"].decode("utf-8"))

hbase_scanner = hbase_table.scan()
books_rdd = sc.parallelize(list(get_books(hbase_scanner)))
books = spark.createDataFrame(books_rdd, schema=["title", "content"])

# Etapa 4: Contar as palavras em cada livro
words = books.select("title", explode(split(books["content"], "\\s+")).alias("word"))
word_counts = words.groupBy("title", "word").count()

# Etapa 5: Salvar os resultados em uma tabela no PostgreSQL
jdbc_url = "jdbc:postgresql://localhost:5432/database"
properties = {"user": "username", "password": "password", "driver": "org.postgresql.Driver"}
word_counts.write.jdbc(url=jdbc_url, table="word_counts", mode="overwrite", properties=properties)


# Parar o SparkContext
sc.stop()



'''

books_rdd = books_rdd.map(lambda x: (os.path.splitext(os.path.basename(x[0]))[0], x[1]))
books = spark.createDataFrame(books_rdd, schema=["title", "content"])

A primeira linha aplica uma transformação de map a um RDD existente chamado books_rdd.
Um RDD é  An RDD is a resilient distributed dataset, que é uma coleção de elementos particionados entre os nós do cluster 
que podem ser operados em paralelo.

Uma transformação de map aplica uma função a cada elemento do RDD e retorna um novo RDD.

A função usada na transformação de map é uma expressão lambda que recebe um elemento x como entrada e retorna uma tupla 
de dois valores: (os.path.splitext(os.path.basename(x[0]))[0], x[1]). 
O elemento x é assumido como outra tupla de dois valores: (x[0], x[1]), onde x[0] é um caminho de arquivo e x1 é algum conteúdo.

O primeiro valor da tupla de saída é obtido aplicando três funções do módulo os.path: basename, splitext e [0]. 
A função basename retorna o componente final de um caminho de arquivo. Por exemplo, os.path.basename("/home/user/file.txt") 
retorna "file.txt". A função splitext divide o caminho do arquivo em uma raiz e uma extensão. 
Por exemplo, os.path.splitext("file.txt") retorna ("file", ".txt"). A função [0] retorna o primeiro elemento de uma sequência. 
Por exemplo, [0]("file", ".txt") retorna "file". Portanto, o primeiro valor da tupla de saída é o nome do arquivo sem a extensão.

O segundo valor da tupla de saída é simplesmente x1, que é o conteúdo associado ao caminho do arquivo.

O resultado de aplicar essa transformação de map ao books_rdd é outro RDD que contém tuplas de (título, conteúdo) para cada livro.

A segunda linha cria um DataFrame a partir do books_rdd usando o método spark.createDataFrame. 
Um DataFrame é uma coleção distribuída de dados organizados em colunas nomeadas. 
É conceitualmente equivalente a uma tabela em um banco de dados relacional ou um quadro de dados em R/Python. 
O argumento schema especifica os nomes das colunas: [“title”, “content”]. 
O resultado dessa linha é um DataFrame chamado books que tem duas colunas: title e content.

'''
