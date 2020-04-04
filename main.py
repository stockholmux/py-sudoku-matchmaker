import redis, random
from fastapi import FastAPI
from service.matchmaking import MatchmakingService

app = FastAPI()
mm = MatchmakingService("mm:", redis.Redis(decode_responses=True))

@app.get("/create_match")
def create_match(p1: str, p2: str):
	match_id = mm.create_match(p1, p2)
	return {"match_id": match_id}

@app.get("/accept_match")
def accept_match(mid: str):
	mm.accept_match(mid)
	return {"ok": True}




	




