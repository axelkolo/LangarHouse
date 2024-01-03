from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.diagnostics import PingState
from couchbase.exceptions import (
        CouchbaseException,
        DocumentExistsException,
        DocumentNotFoundException
)
from couchbase.options import ClusterOptions

# This is the CouchbaseClient class. It's an object-oriented representation of a connection to a Couchbase cluster.
class CouchbaseClient(object):
    def __init__(self, host, username, password, bucket, collection, scope):
        self.host = host
        self.username = username
        self.password = password
        self.bucket = bucket
        self.collection = collection
        self.scope = scope 
    
    # Method to connect to the Couchbase cluster
    def connect(self, **kwargs):
       
        # Construct the connection string 
        conn_str = f"couchbase://{self.host}"
        try:
            # Create cluster options with password authentication
            cluster_options = ClusterOptions(authenticator=PasswordAuthenticator(self.username, self.password))
            # Connect to the cluster
            self.cluster = Cluster(conn_str, cluster_options, **kwargs)  
        except CouchbaseException as error:
            # Print the error and re-raise it
            print(f"Could not connect to cluster. Error: {error}")
            raise
        # Get the bucket from the cluster
        self.bucket = self.cluster.bucket(self.bucket)
        # Get the collection from the bucket
        self.collection = self.bucket.scope(self.scope).collection(self.collection)
    
     # Method to get a document by key    
    def get(self, key):
        return self.collection.get(key)
    
    # Method to insert a new document
    def insert(self, key, doc):
        return self.collection.insert(key, doc)
    
    # Method to upsert a document (insert if not exists, update otherwise)
    def upsert(self, key, doc):
        return self.collection.upsert(key, doc)
    
     # Method to remove a document by key
    def remove(self, key):
        return self.collection.remove(key)
    
    # Method to execute a N1QL query against the Couchbase cluster
    def query(self, strQuery, **kwargs, *options):
        return self.cluster.query(strQuery, **kwargs, *options)