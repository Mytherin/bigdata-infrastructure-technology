# Big Data Infrastructures & Technology Materials

This repository contains the materials used in the Big Data Infrastructure & Technology course.

## Assignment 1

https://docs.google.com/document/d/1dTUuf5k8oLZD2bpzHiorq-06IsJhhmK4JBMl7l4_gx0/edit?usp=sharing

## Assignment 2
https://docs.google.com/document/d/1_9K1Hej3sVVreQDJAV5FimzetC1PvVd1SXaQ7mG_v4I/edit?usp=sharing


Answers:

```
Exercise 1:
nasa.agg({"response_bytes": "avg"}).show()
Exercise 2:
nasa.filter(nasa['status']==200).count() * 100 / nasa.count()
Exercise 3:
grouped = nasa.groupby('remote_host').agg({'remote_host': 'count'})
grouped.orderBy(grouped['count(remote_host)'].desc()).show()
Exercise 4:
grouped = nasa.filter(nasa['status']!=200).groupby('remote_host').agg({'remote_host': 'count'})
grouped.orderBy(grouped['count(remote_host)'].desc()).show()
Exercise 5:
grouped = nasa.groupby('request_url').agg({'request_url': 'count'})
grouped.orderBy(grouped['count(request_url)'].desc()).show()
Exercise 6:
grouped = nasa.groupby('request_url').agg({'response_bytes': 'sum'})
grouped.orderBy(grouped['sum(response_bytes)'].desc()).show()
```

## Assignment 3

https://docs.google.com/document/d/1bcebTpFApbWm8TD14X2KbJiz4kBnfLf0jL4iQEsy9wE/edit?usp=sharing

Answers:

```sql
-- Exercise 1
SELECT flightdate, COUNT(*) AS count
FROM ontime
GROUP BY flightdate
ORDER BY count DESC
LIMIT 5
-- Exercise 2
SELECT carrier, origincityname, destcityname, depdelayminutes AS max_depdelay
FROM ontime
ORDER BY max_depdelay DESC
LIMIT 3
-- Exercise 3
SELECT dayofweek, COUNT(*) AS num_flights
FROM ontime
WHERE month IN (6, 7, 8)
GROUP BY dayofweek
-- Bonus Exercise 1
SELECT carrier, SUM(CASE WHEN depdelayminutes >= 10 THEN 1 ELSE 0 END) / COUNT(*) AS percentage_high_delay
FROM ontime
GROUP BY carrier
ORDER BY percentage_high_delay DESC
LIMIT 3
-- Bonus Exercise 2A
SELECT flightdate, origincityname, destcityname, arrdelayminutes, depdelayminutes, depdelayminutes - arrdelayminutes AS time_made_up
FROM ontime
ORDER BY time_made_up DESC
LIMIT 3
-- Bonus Exercise 2B
SELECT flightdate, origincityname, destcityname, arrdelayminutes, depdelayminutes, arrdelayminutes - depdelayminutes AS time_lost
FROM ontime
ORDER BY time_lost DESC
LIMIT 3
-- Bonus Exercise 3A
SELECT origincityname, destcityname, SUM(diverted) AS total_diverted
FROM ontime
GROUP BY origincityname, destcityname
ORDER BY total_diverted DESC
LIMIT 3
-- Bonus Exercise 3B
SELECT origincityname, destcityname, SUM(cancelled) AS total_cancelled
FROM ontime
GROUP BY origincityname, destcityname
ORDER BY total_cancelled DESC
LIMIT 3
-- Bonus Exercise 4
SELECT carrier, SUM(distance), SUM(cancelled), SUM(cancelled) / SUM(distance) AS cancelled_per_km
FROM ontime
GROUP BY carrier
ORDER BY cancelled_per_km DESC
LIMIT 3
```
