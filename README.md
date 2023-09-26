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
