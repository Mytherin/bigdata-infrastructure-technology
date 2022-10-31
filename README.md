# Big Data Infrastructures & Technology Materials

This repository contains the materials used in the Big Data Infrastructure & Technology course.



## Assignment 1

https://docs.google.com/document/d/1dTUuf5k8oLZD2bpzHiorq-06IsJhhmK4JBMl7l4_gx0/edit?usp=sharing

## Assignment 2
https://docs.google.com/document/d/1_9K1Hej3sVVreQDJAV5FimzetC1PvVd1SXaQ7mG_v4I/edit?usp=sharing


```
Q1: Find the average number of response bytes.

nasa.agg({"response_bytes": "avg"}).show()



Q2: Find the percentage of successful requests (a successful request is one that has a status code of 200).

nasa.filter(nasa['status']==200).count() * 100 / nasa.count()



Q3: Find the remote_host that made the highest amount of requests. How many requests did he make?


grouped = nasa.groupby('remote_host').agg({'remote_host': 'count'})
grouped.orderBy(grouped['count(remote_host)'].desc()).show()



Q4: Find the remote_host that made the highest amount of failed requests (a failed request is one that has a status code not equal to 200). How many failed requests did he make?


grouped = nasa.filter(nasa['status']!=200).groupby('remote_host').agg({'remote_host': 'count'})
grouped.orderBy(grouped['count(remote_host)'].desc()).show()



Q5: What are the top-5 most frequently requested request_urls?


grouped = nasa.groupby('request_url').agg({'request_url': 'count'})
grouped.orderBy(grouped['count(request_url)'].desc()).show()



Q6: What are the top-5  request_urls that have consumed the most bandwidth?


grouped = nasa.groupby('request_url').agg({'response_bytes': 'sum'})
grouped.orderBy(grouped['sum(response_bytes)'].desc()).show()

```


## Assignment 3

https://docs.google.com/document/d/1bcebTpFApbWm8TD14X2KbJiz4kBnfLf0jL4iQEsy9wE/edit?usp=sharing

```

Q1: What date has the highest amount of flights?


spark.sql('''
SELECT flightdate, COUNT(*) AS count
FROM ontime
GROUP BY flightdate
ORDER BY count DESC
LIMIT 5
''').show();


Q2: What are the carrier, origin city and destination city from the three flights with the highest departure delay? How high is that delay in minutes?


spark.sql('''
SELECT carrier, origincityname, destcityname, depdelayminutes
FROM ontime
WHERE depdelayminutes=(SELECT MAX(depdelayminutes) FROM ontime)
''').show();


Q3: In the months of June, July and August, how many flights are there for each day of the week (i.e. how many flights are there on Monday, how many are there on Tuesday, etc)?




spark.sql('''
SELECT dayofweek, COUNT(*)
FROM ontime
WHERE month IN (6, 7, 8)
GROUP BY dayofweek
ORDER BY dayofweek
''').show();


Bonus Q1: What are the three carriers that have the highest percentage of flights with a departure delay of 10 minutes or more, and how high is that percentage?


spark.sql('''
SELECT carrier, SUM(CASE WHEN depdelayminutes>=10 THEN 1 ELSE 0 END)/COUNT(*) AS percentage_delay
FROM ontime
GROUP BY carrier
ORDER BY percentage_delay DESC
LIMIT 3
''').show();


Bonus Q2: What are the three flights that made up the most time lost time during their flight (i.e. the arrival delay is lower than the departure delay)? What are the three flights that lost the most time during their flight?


spark.sql('''
SELECT flightnum, arrdelayminutes, depdelayminutes, depdelayminutes - arrdelayminutes AS time_made_up
FROM ontime
ORDER BY time_made_up DESC
LIMIT 3
''').show();


Bonus Q3: What is the flight route that had the most diverted flights? What is the flight route that had the most cancelled flights?


spark.sql('''
SELECT origincityname, destcityname, SUM(diverted) AS diverted
FROM ontime
GROUP BY origincityname, destcityname
ORDER BY diverted DESC LIMIT 5
''').show();


spark.sql('''
SELECT origincityname, destcityname, SUM(cancelled) AS cancelled
FROM ontime
GROUP BY origincityname, destcityname
ORDER BY cancelled DESC LIMIT 5
''').show();


Bonus Q4: What carrier had the most cancelled flights per kilometer their airplanes have travelled?


spark.sql('''
SELECT carrier, SUM(cancelled) cancelled, SUM(distance) distance_travelled, SUM(cancelled) / SUM(distance) cancelled_flights_per_km
FROM ontime
GROUP BY carrier
ORDER BY cancelled_flights_per_km DESC
''').show();


```

