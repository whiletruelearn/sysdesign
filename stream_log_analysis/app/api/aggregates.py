

from datetime import datetime
from clearml.automation.controller import PipelineDecorator


@PipelineDecorator.component(execution_queue="default")
def get_data_from_postgres():
    from sqlalchemy.orm import declarative_base
    from datetime import date
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy import create_engine
    from sqlalchemy import Column, Integer, String,DateTime
    import pandas as pd

    Base = declarative_base()
    
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

    results = s.query(Logs.arrival_time,Logs.http_code,Logs.ip_address,Logs.http_method).all()
    df = pd.DataFrame(results)
    print(f"Number of logs {df.shape[0]}")
    df.to_parquet("training_data/train.parquet")

@PipelineDecorator.component(execution_queue="default")
def push_data_to_minio(date_str):
    import pandas as pd
    from minio import Minio
    client = Minio(
        "192.168.0.105:9000",
        access_key="4RYYyLXSrGSMAs8f",
        secret_key="jeh7tx11vz5UfCZv6AbNS1O7uK7dShE0",
        secure=False
        
    )
    found = client.bucket_exists("spa-assignment")
    if not found:
        client.make_bucket("spa-assignment")
    else:
        print("Bucket 'spa-assignment' already exists")

    client.fput_object(
        "spa-assignment", f"{date_str}/training-data/train.parquet", "/Users/whiletruelearn/projects/sysdesign/stream_log_analysis/app/api/training_data/train.parquet",
    )
    print("Wrote parquet to minio!")

@PipelineDecorator.component(execution_queue="default")
def get_statistics():
    import pandas as pd
    df = pd.read_parquet("training_data/train.parquet")
    print("Aggregate statistics overall for HTTP methods")
    print(df.groupby("http_method").count()["arrival_time"])

    print("Aggregate statistics overall for HTTP codes")
    print(df.groupby("http_code").count()["arrival_time"])



 
    
@PipelineDecorator.pipeline(name='aggregates-pipeline', project='spa',version='0.1')
def main_pipeline():
    date_str = str(datetime.now())
    get_data_from_postgres()
    push_data_to_minio(date_str)
    get_statistics()


if __name__ == '__main__':
    PipelineDecorator.run_locally()
    main_pipeline()


# from clearml import PipelineDecorator

# @PipelineDecorator.component(cache=True, execution_queue="default")
# def step(size: int):
#     import numpy as np
#     return np.random.random(size=size)

# @PipelineDecorator.pipeline(
#     name='aggregates',
#     project='data processing',
#     version='0.1'
# )
# def pipeline_logic(do_stuff: bool):
#     if do_stuff:
#         return step(size=42)

# if __name__ == '__main__':
#     # run the pipeline on the current machine, for local debugging
#     # for scale-out, comment-out the following line and spin clearml agents
#     PipelineDecorator.run_locally()

#     pipeline_logic(do_stuff=True)