import pymongo
import certifi
import pandas as pd
from ast import literal_eval
from functools import reduce
import sqlite3
import logging
import os
from rich.console import Console

COLS_TO_CACHE = ['_id', 'listing_url', 'name', 'summary', 'space', 'description',
       'neighborhood_overview', 'notes', 'transit', 'access', 'interaction',
       'house_rules', 'property_type', 'room_type', 'bed_type',
       'minimum_nights', 'maximum_nights', 'cancellation_policy',
       'last_scraped', 'calendar_last_scraped', 'first_review', 'last_review',
       'accommodates', 'bedrooms', 'beds', 'number_of_reviews', 'bathrooms',
       'amenities', 'price', 'security_deposit', 'cleaning_fee',
       'extra_people', 'guests_included', 'images', 'host', 'address',
       'availability', 'review_scores', 'reviews', 'weekly_price',
       'monthly_price', 'reviews_per_month']

USER = "bitsXXXXXX"
PWD = "bitsXXXXXXXX"
URL = f"mongodb+srv://{USER}:{PWD}@cluster0.r5hrz.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"



class MongoConnector:
    def __init__(self,url):
        self.url = url
        self.collection = None

    def connect(self,db_name='sample_airbnb',collection_name='listingsAndReviews'):
        myclient = pymongo.MongoClient(self.url, tlsCAFile=certifi.where())
        db = myclient[db_name] #sample_airbnb
        self.collection = db[collection_name] #listingsAndReviews
        return True
        

    def execute_query_subset(self, query):
        
        res = pd.DataFrame(list(self.collection.find(query)))
        res = res.apply(lambda x : x.astype(str), axis =1)
        return res


class Query:
    def __init__(self,query_str):
        self.query_str = query_str
        self.query = literal_eval(self.query_str)
        
    def get_filter_conditions(self):
        filter_conditions = self.query[0]
        filter_concact_type = list(filter_conditions["$match"].keys())[0]
        if filter_concact_type not in ["$or","$and"]:
            raise ValueError(f"{filter_concact_type} has to be either or and and")
        
        filters = filter_conditions["$match"][filter_concact_type]
        return filter_concact_type, filters

    def get_projections(self):
        projections = self.query[1]
        return list(projections["$project"].keys())


    



class QueryLoader:
    def __init__(self,path):
        self.path = path 

    def _read_input_json(self):
        df = pd.read_json(self.path)
        return df 
    
    def get_queries(self):
        df = self._read_input_json()
        for _,row in df.iterrows():
            query = row["query"]
            yield query 

class SQLQuery:
    def __init__(self):
        self.projections = None
        self.filters = []
        self.concat_type = None
        self.table = None

    def add_filter(self,filter):
        self.filters.append(filter)


    def set_projections(self, projections):
        self.projections = projections

    def set_concat_type(self,concat_type):
        self.concat_type = concat_type

    def set_table(self,table):
        table = table.replace(" ","_").replace("/","_")
        self.table = table


    def generate_sql(self):
        
        
        filter_str = []
        filter_attrib = []
        for d in self.filters:
            for k,v in d.items():
                filter_attrib.append(k)
                filter_str.append(f"{k}='{v}'")

    
        filter_str = f"{self.concat_type}".join(filter_str)
            
        SQL_STR = f"SELECT * FROM {self.table} where {filter_str}"
        return SQL_STR


class LocalCacheSQLite:
    def __init__(self,cache_db_path='test.db',size_limit=1000):
        self.con = sqlite3.connect(cache_db_path)
        self.cur = self.con.cursor()
        self.size_limit = size_limit
        self.curr_cache = []
        self.cache_hit = 0
        self.cache_miss = 0
        self.db_count = 0 



    def close_connection(self):
        self.con.close()

    def _get_cache_id(self,filters):
        cache_id  = "_".join([(k,str(filters[k])) for k in filters][0])
        cache_id = cache_id.replace(" ","_").replace("/","_")
        return cache_id

    def get_cache_data(self,sql_query,cache_id):
        self.curr_cache.remove(cache_id)
        self.curr_cache.append(cache_id)
        cache_res =  pd.DataFrame(list(self.cur.execute(sql_query)))
        return cache_res
        
    
    def insert_data(self,subset_data,cache_id):
        if len(self.curr_cache) >= self.size_limit:
            self._delete_data()
        subset_data.to_sql(name=cache_id,con = self.con, chunksize =10,index=False)
        self.curr_cache.append(cache_id)

    def _delete_data(self):
        cache_to_remove = self.curr_cache.pop(0)
        self.cur.execute(f"DROP TABLE {cache_to_remove}")
        print(f"Table dropped... {cache_to_remove}")

    def increment_cache_hit(self):
        self.cache_hit +=1 

    def increment_cache_miss(self):
        self.cache_miss +=1 


    

class DataManipulator:
    def __init__(self,concat_type,projections,remove_duplicates=True):
        self.concat_type = concat_type
        self.projections = projections
        self.remove_duplicates = remove_duplicates
        self.dfs = []
        

    def add_dfs(self,df):
        self.dfs.append(df)

    def get_combined_df(self):
        output = None
        if self.concat_type == "$or":
            output = pd.concat(self.dfs,axis=0)
        elif self.concat_type == "$and":
            
            output = reduce(lambda  left,right: pd.merge(left,right,on=self.projections, how='inner'),self.dfs)
        
        if self.remove_duplicates:
            output.drop_duplicates(inplace=True)
        
        return output 
    

def run_main():
    if os.path.exists("test.db"):
        os.remove("test.db")
    
    console = Console()
    logger = logging.getLogger('sda')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    logger.addHandler(handler)
    
    console.print("Starting Main", style="bold red")
    mongo_connector = MongoConnector(URL)
    mongo_connector.connect()
    console.print("Connected to MongoDB", style="bold red")
    query_loader = QueryLoader("input.json")
    queries = query_loader.get_queries()
    cache = LocalCacheSQLite()
    
    for query_id,query in enumerate(queries):
        
        console.print(f"Query id {query_id}",style = "bold magenta")
        console.print(f"processing Query {query}",style = "bold magenta")
        q = Query(query)
        
        filter_concat_type, filters = q.get_filter_conditions()
        projections = q.get_projections()
        dm = DataManipulator(concat_type=filter_concat_type,projections=projections)
        for filter in filters:
            sq = SQLQuery()
            sq.set_projections(projections)
            cache_id = cache._get_cache_id(filter)
            if cache_id in cache.curr_cache:
                console.print(f"Filter {filter} , Projections {projections} found in cache as {cache_id}. Retrieving data from cache",style= "bold green")   
                sq.set_table(cache_id)
                sq.add_filter(filter)
                sql = sq.generate_sql()
                console.print(f"SQL query generated {sql}",style= "bold green")
                cache_res = cache.get_cache_data(sql,cache_id)
                cache_res.columns = COLS_TO_CACHE
                cache.increment_cache_hit()
                console.print(f"Cache hit counter :{cache.cache_hit}",style= "bold blue")
                dm.add_dfs(cache_res)
            else:
                console.print(f"Filter {filter} and Projections {projections} not found in cache as {cache_id}. Retrieving data from MongoDB",style= "bold green")
                subset_result = mongo_connector.execute_query_subset(filter) 
                subset_result.columns = COLS_TO_CACHE
                cache.increment_cache_miss()
                console.print(f"Cache Miss counter :{cache.cache_miss}",style= "bold blue")
                cache.insert_data(subset_result,cache_id)
                dm.add_dfs(subset_result)
        
        final_result = dm.get_combined_df()[projections]
        console.print(f"Query had given a resultset of shape {final_result.shape}", style = "bold yellow")
        console.print(f"cache_hit : {cache.cache_hit} cache_miss : {cache.cache_miss}", style = "bold yellow")
        console.print(f"Number of entries in cache {len(cache.curr_cache)}",style="bold purple")
    cache.close_connection()
    console.print("Closing Connection",style="bold red")
if __name__ == "__main__":
    run_main()
    
