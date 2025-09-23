# Minimal db.py for RankAndReasoning Lambda
from config import mongo_client, mongo_db
from logging_config import setup_logger

logger = setup_logger(__name__)

# MongoDB client setup - using configured client from config.py
client = mongo_client
db = mongo_db

# MongoDB collections actually used by RankAndReasoning
searchOutputCollection = db["searchOutput"]
nodes_collection = db["node"]

# Create only the essential indexes that RankAndReasoning actually needs
searchOutputCollection.create_index([
    ("userId", 1),
    ("createdAt", -1)
])

nodes_collection.create_index([
    ("userId", 1),
    ("_id", 1)
])