

#------------------ENCONTRANDO EL AEROPUERTO------------------------

#carga de datos

alist = spark.read\
    .format("com.databricks.spark.csv")\
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .load("C:/sparkPracticas/airports/airportList.txt")

aloc = spark.read\
    .format("com.databricks.spark.csv")\
    .option("header", "true") \
    .option("delimiter", ",") \
    .option("inferSchema", "true") \
    .load("C:/sparkPracticas/airports/airportsLocation.txt")


result = alist \
    .join(aloc, \
        (alist.airport_code == aloc.airport_code ),
        "inner"
        ) \
    .select( \
    alist.airport_id,
    aloc.airport_code,
    alist.airport_name,
    alist.city_airport_location,
    alist.country,
    aloc.latitude,
    aloc.longitude
    )

#ONE WAY

result1=result.filter((result.latitude>=40)& (result.airport_code!=""))
result1.show()

#ANOTHER WAY
result1=result.select( \
    alist.airport_id,
    aloc.airport_code,
    alist.airport_name,
    alist.city_airport_location,
    alist.country,
    aloc.latitude,
    aloc.longitude
    ).where(aloc.latitude>=40).show()




#------------------ALERTA DE TERRORISMO------------------------

result2=result.filter(((result.airport_id % 2) != 0 )& ((result.country == "Mexico") | (result.country == "United States") |
              (result.country == "Brazil") | (result.country == "Canada") | (result.country =="Japan")))\
    .sort(result.country, ascending=True)

result2.show(100)