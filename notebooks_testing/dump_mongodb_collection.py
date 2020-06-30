import pymongo
import pandas as pd

class DumpMongoCollection:
    def __init__(self, database: str):
        self.database = database
        self.monogdb_ip = "host.docker.internal"
        self.monogdb_port = 27017

    def extract_pandas(self, collection: str):
        client = pymongo.MongoClient(self.monogdb_ip , port = self.monogdb_port)
        db = client[self.database]
        data = pd.DataFrame(list(db[collection].find()))
        return data


