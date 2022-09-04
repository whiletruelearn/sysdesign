from ipaddress import ip_address
import faust
import re
from pydantic import BaseModel
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String,DateTime
from datetime import datetime
import uuid
from sqlalchemy.orm import declarative_base
from datetime import date
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

app = faust.App('faust-streaming', broker='kafka://localhost:9092',value_serializer='raw')

logs_kafka_topic = app.topic('test')
engine = create_engine('postgresql+psycopg2://postgres:BfwPmUYpay@localhost:5432/spa')

Session = sessionmaker(bind=engine)

s = Session()

class Logs(Base):
    __tablename__ = 'logs'
    __table_args__ = {'extend_existing': True} 
    id = Column(String,primary_key=True)
    arrival_time = Column(DateTime)
    ip_address = Column(String)
    url = Column(String)
    http_code = Column(String)
    http_method = Column(String)



class LogLine(BaseModel):
    ip_address : str
    url : str 
    http_code : str 
    http_method : str

def parse(obj):
    obj_str =  obj.decode("utf8")
    logline = LogLine(ip_address=obj_str.split(" - -")[0].strip(),
                url = re.search("(?P<url>https?://[^\s]+)", obj_str).group("url"),
                http_code = obj_str.split("\"")[2].split(" ")[1],
                http_method = obj_str.split("\"")[1].split(" ")[0].strip()
                )
    return logline

def recreate_database():
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

recreate_database()



@app.agent(logs_kafka_topic)
async def process_logs(logs):
    async for log in logs:

        logline: LogLine = parse(log)
        l = Logs( id = uuid.uuid4().hex,
            arrival_time = datetime.now(),
            ip_address= logline.ip_address,
            url = logline.url,
            http_code= logline.http_code,
            http_method= logline.http_method)
        
        s.add(l)
        s.commit()





