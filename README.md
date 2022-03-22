# Big Data Infrastructures & Technology Materials

This repository contains the materials used in the Big Data Infrastructures & Technology course.



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


```sql

-- Exercise 1
SELECT customer.cid, customer.name, SUM(used_power) total_used
FROM customer
JOIN smartmeter ON (customer.cid=smartmeter.cid)
JOIN poweruse ON (smartmeter.serialno=poweruse.serialno)
GROUP BY customer.cid, customer.name
ORDER BY total_used DESC
LIMIT 1

-- Exercise 2

SELECT customer.postcode, SUM(used_power) total_used
FROM customer
JOIN smartmeter ON (customer.cid=smartmeter.cid)
JOIN poweruse ON (smartmeter.serialno=poweruse.serialno)
GROUP BY customer.postcode
ORDER BY customer.postcode

-- Exercise 3
SELECT month, AVG(used_power_monthly) monthly_power
FROM (
   SELECT month, SUM(used_power) AS used_power_monthly
   FROM customer
   JOIN smartmeter ON (customer.cid=smartmeter.cid)
   JOIN poweruse ON (smartmeter.serialno=poweruse.serialno)
   GROUP BY year, month)
GROUP BY month
ORDER BY monthly_power DESC
LIMIT 3

-- Exercise 4
SELECT AVG(used_power_monthly)
FROM (
   SELECT month, SUM(used_power) AS used_power_monthly
   FROM customer
   JOIN smartmeter ON (customer.cid=smartmeter.cid)
   JOIN poweruse ON (smartmeter.serialno=poweruse.serialno)
   GROUP BY year, month)
   
-- Bonus, Exercise 1
SELECT customer.cid, customer.name, SUM(used_power) used_power
FROM customer
JOIN smartmeter ON (customer.cid=smartmeter.cid)
JOIN poweruse ON (smartmeter.serialno=poweruse.serialno)
WHERE year>=2018
GROUP BY customer.cid, customer.name
ORDER BY used_power DESC
LIMIT 10;

-- Bonus, Exercise 2
SELECT day, month, year, sum(used_power) as sup
FROM poweruse
WHERE year=2019
GROUP BY day, month, year
ORDER BY sum(used_power) DESC
LIMIT 1

-- Bonus, Exercise 3

SELECT serialno, year, sum(used_power)
FROM poweruse
GROUP BY serialno, year
HAVING sum(used_power)>20000;

