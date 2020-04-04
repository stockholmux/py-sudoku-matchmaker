#!/usr/bin/env python3
import sys
from service.matchmaking import MatchmakingService

if len(sys.argv) != 2:
	print("Usage: ./commit_worker.py <uniqueInstanceID>")
	exit(1)
instanceID = sys.argv[1]

r = None
if False:
	from rediscluster import RedisCluster
	r = RedisCluster(startup_nodes=[{'host': 'localhost', 'port': 7000}], decode_responses=True)
else:
	from redis import Redis
	r = Redis("localhost", 6379, decode_responses=True)


mm = MatchmakingService("mm:", r)
mm.start_processing_commit_stream(instanceID)
