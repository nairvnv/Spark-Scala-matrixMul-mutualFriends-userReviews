from pyspark.mllib.linalg import DenseMatrix
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col
from pyspark.mllib.linalg.distributed import IndexedRowMatrix
from pyspark.sql.types import *
from pyspark.sql.functions import monotonically_increasing_id 


sc = SparkContext("local","matrix")
spk = SparkSession(sc)

#matrix1 import and map
mat1 = spk.read.text("./dataset/matrix1 - small.txt")
mat1 = mat1.select("*").withColumn("id", monotonically_increasing_id())
mat1 = IndexedRowMatrix(mat1.rdd.map(lambda x: (int(x[1]) , [int(x[1])] + list(x[0].replace(" ","")))))

#matrix 2 import and map
mat2 = spk.read.text("./dataset/matrix2 - small.txt")
mat2_len = len(mat2.take(1)[0].value.replace(" ","").split())

mat2_mod = [1] + [0] * 100
for i in mat2.collect():
    mat2_mod += [int(0)] + i.value.split()

#perform multiplication using mllibs DenseMatrix
mat2 = DenseMatrix( 101, 101 , mat2_mod, isTransposed=True)
ratings_matrix = mat1.multiply(mat2)
print(ratings_matrix.rows.collect())