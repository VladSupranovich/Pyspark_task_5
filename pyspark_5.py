import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import findspark
findspark.init()

import data_frames as df



#1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.
df_task_1 = (df.df_category.join(df.df_film_category, "category_id")
                .groupBy("name")
                .agg(count("film_id").alias("num_of_films"))
                .orderBy("num_of_films", ascending=False)
            )

#df_task_1.show()



#2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.
df_task_2 = (df.df_actor.alias('a').join(df.df_film_actor.alias('fa'), "actor_id")
.join(df.df_inventory.alias('i'), col("fa.film_id") ==  col("i.film_id"))
.join(df.df_rental.alias('r'), col("i.inventory_id") ==  col("r.inventory_id"))
.groupBy('a.actor_id', "a.first_name", "a.last_name")
    .agg(concat_ws(" ", "a.first_name", "a.last_name").alias("full_name"),
        count("r.rental_id").alias("num_of_films"))
        .orderBy("num_of_films", ascending=False).select('full_name', 'num_of_films')
             )

#df_task_2.show()



#3. Вывести категорию фильмов, на которую потратили больше всего денег.
df_task_3 = (df.df_category.alias('c').join(df.df_film_category.alias('fc'), col('c.category_id') == col('fc.category_id'))
             .join(df.df_film.alias('f'), col('fc.film_id') == col('f.film_id'))
             .join(df.df_inventory.alias('i'), col('f.film_id') == col('i.film_id'))
             .join(df.df_rental.alias('r'), col('i.inventory_id') == col('r.inventory_id'))
             .join(df.df_payment.alias('p'), col('r.rental_id') == col('p.rental_id'))
             .groupBy('c.name')
             .agg(sum('p.amount').alias('amount'))
             .orderBy('amount', ascending=False).limit(1)
             )

#df_task_3.show()



#4. Вывести названия фильмов, которых нет в inventory.
df_task_4 = (df.df_film.alias('f').select('film_id',"title") \
                .join(df.df_inventory.alias('i'), col("f.film_id") == col("i.film_id"), "leftanti") \
                .distinct().select('title')
)

#df_task_4.show()



#Вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”.
#Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

df_task_5 = (df.df_actor.alias('a')
             .join(df.df_film_actor.alias('fa'), col('a.actor_id') == col('fa.actor_id'))
             .join(df.df_film.alias('f'), col('fa.film_id') == col('f.film_id'))
             .join(df.df_film_category.alias('fc'),col('f.film_id') == col('fc.film_id'))
             .join(df.df_category.alias('c'), col('fc.category_id') == col('c.category_id')).filter(col('c.name')  == 'Children')
             .groupBy("a.first_name", "a.last_name")
             .agg(count("f.film_id").alias("num_of_films"))
             .withColumn("full_name", concat(col("a.first_name"), lit(" "), col("a.last_name")))
             .withColumn("rating", rank().over(Window.orderBy(col("num_of_films").desc())))
             .filter(col("rating") <= 3)
             .select("full_name")
)

#df_task_5.show()



#6. Вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1).
#Отсортировать по количеству неактивных клиентов по убыванию.

df_task_6 = (df.df_city.alias('c')
             .join(df.df_address.alias('a'), col('c.city_id') == col('a.city_id'))
             .join(df.df_store.alias('s'), col('a.address_id') == col('s.address_id'))
             .join(df.df_customer.alias('cs'), col('s.store_id') == col('cs.store_id'))
             .groupBy("c.city")
             .agg(
    count(when(col("cs.active") == 1, col("cs.customer_id"))).alias("active"),
    count(when(col("cs.active") == 0, col("cs.customer_id"))).alias("inactive"))
             .orderBy("inactive", ascending=False)
)

#df_task_6.show()



#7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city),
#и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”.

tab = (
    df.df_category.alias('c')
    .join(df.df_film_category.alias('fc'), col('c.category_id') == col('fc.category_id'))
    .join(df.df_film.alias('f'), col('fc.film_id') == col('f.film_id'))
    .join(df.df_inventory.alias('i'), col('f.film_id') == col('i.inventory_id'))
    .join(df.df_rental.alias('r'), col('i.inventory_id') == col('r.inventory_id'))
    .join(df.df_customer.alias('cs'), col('r.customer_id') == col('cs.customer_id'))
    .join(df.df_address.alias('a'), col('cs.address_id') == col('a.address_id'))
    .join(df.df_city.alias('ct'), col('a.city_id') == col('ct.city_id'))
)


result_1 = (tab.filter((lower(col("city")).like("a%")))
    .groupBy('name')
    .agg(sum(col('return_date') - col('rental_date')).alias('hours_rent'))
    .withColumn("max_hours_rent", max('hours_rent').over(Window.orderBy(desc('hours_rent'))))
    .select('name')
    .filter(col('hours_rent') == col('max_hours_rent')))


result_2 = (tab.filter((lower(col("city")).like("%-%")))
    .groupBy('name')
    .agg(sum(col('return_date') - col('rental_date')).alias('hours_rent'))
    .withColumn("max_hours_rent", max('hours_rent').over(Window.orderBy(desc('hours_rent'))))
    .select('name')
    .filter(col('hours_rent') == col('max_hours_rent')))

df_task_7 = result_1.union(result_2)

#df_task_7.show()
