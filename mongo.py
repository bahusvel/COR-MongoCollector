from cor.api import CORModule, Message
import struct
import ipaddress
import pymongo


class MongoCollector(CORModule):

	def sensor_reading(self, message):
		ipid = "/" + self.locationID
		for src in message.source[:-1]:
			addr = ipaddress.ip_address(struct.unpack(">I", src))
			ipid += "/" + addr
		if ipid not in self.db.collection_names():
			self.db.create_collection(ipid, capped=True, size=100000000)
			collection = self.db[ipid]
			collection.create_index([("type", pymongo.HASHED)])
			collection.create_index([("timestamp", pymongo.DESCENDING)])
		collection = self.db[ipid]
		collection.insert_one(message.payload)

	def __init__(self, locationID=None, mongostring="mongodb://localhost:27017/", *args, **kwargs):
		super().__init__(*args, **kwargs)
		if locationID is None:
			pass  # query manager for his location
		self.locationID = locationID
		self.client = pymongo.MongoClient(mongostring)
		self.db = self.client.sensors
		print("Initializing Mongo Collector " + str(self.mid))
		self.add_topics({"SENSOR_READING": self.sensor_reading})
