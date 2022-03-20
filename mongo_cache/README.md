A simple cache for MongoDB with limited support for query rewrite.


### Install the dependencies

The dependencies needed are 

```
pymongo
certifi
pandas
sqlite3
rich
```

### Run program

Run the command ```python assignment.py```


### Solution Aproach

- We use `mongodb` as the backend store and `sqlite` as the embedded cache sqldb. 

- Queries are read  from `input.json` one by one and executed. 

- We parse the mongodb query and extract the different filter criterias, and for each filter criteria we cache it individually
  into sqlite table. We create a `cache_id` that represents the filter criteria. This is an advantage when a query multiple filters and has one of 
  the filters available in cache, it leads to a cache hit. We would only need to scan from mongodb for the filters which are not available in cache.

- Since we may get partial data from Mongodb and  partial data from sqlite, we do the data manipulation in pandas. `and` operation is executed through 
  a pandas join, `or` operation is completed through pandas concat.

- Cache size is something that could be specified while instantiating the cache object. We use a Least recently used approach to delete the cache
  tables from sqlite. We use the property of list to maintain the order of arrival of cache objects. We swap out their positions when there is a cache-hit. 

- All the information such as cache_hit, cache_miss , details on when we are hitting mongodb etc are logged. 

Note : We also convert the data from MongoDB to string type after reading , since some types were nested and not getting inserted to sqlite. This casting operation ensures
that we are able to write data to sqlite properly.


### Testing 

- We had 17 filter conditions as part of the queries. 
- We noticed a cache hit of 12 and cache miss of 5 in our experiments. This was expected as in our dry run. 
- Cache hit ~ 70% , cache miss ~ 30%
- Can see speed up during cache hit, and query executiont taking more time during cache miss.


### Logs 

```
Starting Main
Connected to MongoDB
Query id 0
processing Query [{'$match':{'$or':[{'property_type':'House'},{'room_type' : 'Private room'}]},},{'$project':{'name':1,'_id':0}}]
Filter {'property_type': 'House'} and Projections ['name', '_id'] not found in cache as property_type_House. Retrieving data from MongoDB
Cache Miss counter :1
Filter {'room_type': 'Private room'} and Projections ['name', '_id'] not found in cache as room_type_Private_room. Retrieving data from MongoDB
Cache Miss counter :2
Query had given a resultset of shape (2589, 2)
cache_hit : 0 cache_miss : 2
Query id 1
processing Query [{'$match':{'$and':[{'property_type':'House'},{'room_type' : 'Private room'}]},},{'$project':{'name':1,'_id':0}}]
Filter {'property_type': 'House'} , Projections ['name', '_id'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :1
Filter {'room_type': 'Private room'} , Projections ['name', '_id'] found in cache as room_type_Private_room. Retrieving data from cache
SQL query generated SELECT * FROM room_type_Private_room where room_type='Private room'
Cache hit counter :2
Query had given a resultset of shape (267, 2)
cache_hit : 2 cache_miss : 2
Query id 2
processing Query [{'$match':{'$and':[{'property_type':'House'},{'room_type' : 'Private room'},{'bed_type': 'Real Bed'}]},},{'$project':{'name':1,'_id':0,'bed_type' : 1}}]
Filter {'property_type': 'House'} , Projections ['name', '_id', 'bed_type'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :3
Filter {'room_type': 'Private room'} , Projections ['name', '_id', 'bed_type'] found in cache as room_type_Private_room. Retrieving data from cache
SQL query generated SELECT * FROM room_type_Private_room where room_type='Private room'
Cache hit counter :4
Filter {'bed_type': 'Real Bed'} and Projections ['name', '_id', 'bed_type'] not found in cache as bed_type_Real_Bed. Retrieving data from MongoDB
Cache Miss counter :3
Query had given a resultset of shape (266, 3)
cache_hit : 4 cache_miss : 3
Query id 3
processing Query [{'$match':{'$or':[{'property_type':'House'},{'room_type' : 'Private room'},{'bed_type': 'Real Bed'}]},},{'$project':{'name':1,'_id':0,'bed_type' : 1}}]
Filter {'property_type': 'House'} , Projections ['name', '_id', 'bed_type'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :5
Filter {'room_type': 'Private room'} , Projections ['name', '_id', 'bed_type'] found in cache as room_type_Private_room. Retrieving data from cache
SQL query generated SELECT * FROM room_type_Private_room where room_type='Private room'
Cache hit counter :6
Filter {'bed_type': 'Real Bed'} , Projections ['name', '_id', 'bed_type'] found in cache as bed_type_Real_Bed. Retrieving data from cache
SQL query generated SELECT * FROM bed_type_Real_Bed where bed_type='Real Bed'
Cache hit counter :7
Query had given a resultset of shape (7492, 3)
cache_hit : 7 cache_miss : 3
Query id 4
processing Query [{'$match':{'$or':[{'property_type':'House'},{'room_type' : 'Entire home/apt'}]},},{'$project':{'name':1,'_id':0,'cancellation_policy':1}}]
Filter {'property_type': 'House'} , Projections ['name', '_id', 'cancellation_policy'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :8
Filter {'room_type': 'Entire home/apt'} and Projections ['name', '_id', 'cancellation_policy'] not found in cache as room_type_Entire_home_apt. Retrieving data from MongoDB
Cache Miss counter :4
Query had given a resultset of shape (3765, 3)
cache_hit : 8 cache_miss : 4
Query id 5
processing Query [{'$match':{'$and':[{'property_type':'House'},{'room_type' : 'Entire home/apt'}]},},{'$project':{'name':1,'_id':0,'cancellation_policy':1}}]
Filter {'property_type': 'House'} , Projections ['name', '_id', 'cancellation_policy'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :9
Filter {'room_type': 'Entire home/apt'} , Projections ['name', '_id', 'cancellation_policy'] found in cache as room_type_Entire_home_apt. Retrieving data from cache
SQL query generated SELECT * FROM room_type_Entire_home_apt where room_type='Entire home/apt'
Cache hit counter :10
Query had given a resultset of shape (330, 3)
cache_hit : 10 cache_miss : 4
Query id 6
processing Query [{'$match':{'$or':[{'property_type':'House'}]},},{'$project':{'name':1,'_id':0}}]
Filter {'property_type': 'House'} , Projections ['name', '_id'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :11
Query had given a resultset of shape (606, 2)
cache_hit : 11 cache_miss : 4
Query id 7
processing Query [{'$match':{'$or':[{'property_type':'House'},{'accommodates': 6}]},},{'$project':{'name':1,'_id':0}}]
Filter {'property_type': 'House'} , Projections ['name', '_id'] found in cache as property_type_House. Retrieving data from cache
SQL query generated SELECT * FROM property_type_House where property_type='House'
Cache hit counter :12
Filter {'accommodates': 6} and Projections ['name', '_id'] not found in cache as accommodates_6. Retrieving data from MongoDB
Cache Miss counter :5
Query had given a resultset of shape (1111, 2)
cache_hit : 12 cache_miss : 5
Closing connection
```