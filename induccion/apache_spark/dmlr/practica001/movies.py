import pandas as pd
from pyspark.sql import functions as F

#------------------ENCONTRANDO EL AEROPUERTO------------------------}


movies = spark.read.load("C:/sparkPracticas/movies/movies.dat",format="csv", sep="::", inferSchema="true", header="true")
ratings = spark.read.load("C:/sparkPracticas/movies/ratings.dat",format="csv", sep="::", inferSchema="true", header="true")
users = spark.read.load("C:/sparkPracticas/movies/users.dat",format="csv", sep="::", inferSchema="true", header="true")

movies = spark.read.option("delimiter", "::").csv('C:/sparkPracticas/movies/movies.dat')

#------------------Rating menor a 4------------------------

result = movies \
    .join(ratings, \
        (movies.MovieID == ratings.MovieID),
        "inner"
        ) \
    .join(users, \
        (ratings.UserID == users.UserID ),
        "inner"
        ) \
    .select( \
    ratings.UserID,
    movies.MovieID,
    ratings.Rating,
    movies.Title,
    movies.Genres,
    users.Age)
result.show()


#------------------Rating menor a 4------------------------
result2=result.filter(result.Rating<4)
result2.show()

#------------------Promociones------------------------


result3=result.filter((result.Genres.contains("Animation"))& (result.Rating>4))
result4=result3.toPandas()

bins = [0,18,24,34,44,49,55,100]
labels = ['1','18','25','35','45','50','56']
b = pd.cut(result4.Age, bins=bins, labels=labels, include_lowest=True)
result4['AgeGroup'] = pd.cut(result4.Age, bins=bins, labels=labels, include_lowest=True)
print (result4)

result5 = result3.groupby(['Genres', b]).size().unstack(fill_value=0).stack().reset_index(name='count')