from typing import Dict
import threading, random

POINTS_STREAM_PARTITIONS = GAME_STREAM_PARTITIONS = COMMIT_PARTITIONS = 3
GROUP_NAME = "matchmaking"


class MatchmakingService:
	def __init__(self, keyNS, redis):
		self.redis = redis
		self.keyNS = keyNS
		self.lock = self.redis.register_script("""
			local lock_key = KEYS[1]
			local lock_id = ARGV[1]

			local lock = redis.call("GET", lock_key)
			if not lock then
				redis.call("SET", lock_key, lock_id)
				return 1
			end

			if lock == lock_id then
				return 1
			end

			return 0
		""")

		self.unlock = self.redis.register_script("""
			local lock_key = KEYS[1]
			local lock_id = ARGV[1]

			local lock = redis.call("GET", lock_key)
			if lock == lock_id then
				redis.call("DEL", lock_key)
			end
		""")

	def get_all_matches(self, player_id):
		#TODO
		pass

	def get_match(self, match_id):
		match_key = self.keyNS + "match:" + match_id
		return self.redis.hmget(match_key)

	def create_match(self, p1, p2):
		# Choose a random partition of the commit stream to update
		commit_partition_number = random.choice(range(COMMIT_PARTITIONS))
		print('partition number {}'.format(commit_partition_number))
		partition = add_slot(self.keyNS + "commit", commit_partition_number)

		# Write to the internal commit stream and trigger the first step of the saga
		match_id = self.redis.xadd(partition, {"p1": p1, "p2": p2, "game": "sudoku", "action": "MUST_LOCK_P1"})
		# return match_id
		return f'{match_id}-{{{commit_partition_number}}}'

	def accept_match(self, match_id):
		match_key = self.keyNS + "match:" + match_id
		match = self.redis.hgetall(match_key)
		state = match["state"]
		if state == "WAITING_P2":
			# Choose a random partition of the commit stream to update
			partition = add_slot(self.keyNS + "commit", random.choice(range(COMMIT_PARTITIONS)))

			new_action = 'MUST_LOCK_P2'
			self.redis.xadd(partition, {"match_id": match_id, "p1": match["p1"], "p2": match["p2"], "game": match["game"], "action": new_action})


	def decline_match(self):
		match_key = self.keyNS + "match:" + match_id
		match = self.redis.hgetall(match_key)
		state = match["state"]
		if state == "WAITING_P2":
			# Choose a random partition of the commit stream to update
			partition = add_slot(self.keyNS + "commit", random.choice(range(COMMIT_PARTITIONS)))

			new_action = 'MUST_FAIL_MATCH'
			self.redis.xadd(partition, {"match_id": match_id, "p1": match["p1"], "p2": match["p2"], "game": match["game"], "action": new_action})


	def process_match(self, commit_stream, entry_id, match):
		p1 = match["p1"]
		p2 = match["p2"]
		game = match["game"]
		match_id = match["match_id"]
		action = match["action"]
		
		match_key = self.keyNS + "match:" + match_id
		if action == "MUST_LOCK_P1":
			p1_lock = self.keyNS + "player:" + p1 + ":lock" 
			
			# Lock P1
			success = self.lock(keys=[p1_lock], args=[match_id])
			if not success:
				new_action = 'MUST_FAIL_MATCH'
				partition = add_slot(self.keyNS + "commit", random.choice(range(COMMIT_PARTITIONS)))
				self.redis.xadd(partition, {"match_id": match_id, "p1": match["p1"], "p2": match["p2"], "game": match["game"], "action": new_action})
				self.redis.xack(commit_stream, GROUP_NAME, entry_id)
				return

			# Create a corresponding match key
			new_state = "WAITING_P2"
			self.redis.hmset(match_key, {"p1": p1, "p2": p2, "game": game, "state": new_state})

			# Add the match to the list of pending matches for p1 and p2
			# (this is necessary to keep clients informed about pending invites)
			p1_matches_key = self.keyNS + "player:" + p1 + ":matches"
			p2_matches_key = self.keyNS + "player:" + p2 + ":matches"
			self.redis.sadd(p1_matches_key, match_key)
			self.redis.sadd(p2_matches_key, match_key)

			# ACK!
			self.redis.xack(commit_stream, GROUP_NAME, entry_id)
			return

		elif action == "MUST_LOCK_P2":
			# We must lock P2 and wait for sudoku-engine to have created the match
			p2_lock = self.keyNS + "player:" + p2 + ":lock" 
			
			# Lock P2
			success = self.lock(keys=[p2_lock], args=[match_id])
			if not success:
				new_action = 'MUST_FAIL_MATCH'
				partition = add_slot(self.keyNS + "commit", random.choice(range(COMMIT_PARTITIONS)))
				self.redis.xadd(partition, {"match_id": match_id, "p1": match["p1"], "p2": match["p2"], "game": match["game"], "action": new_action})
				self.redis.xack(commit_stream, GROUP_NAME, entry_id)
				return

			# Set the match to the next state
			new_state = "WAITING_GAME"
			self.redis.hmset(match_key, {"state": new_state})

			# Publish that the match is ready for consumption by the corresponding game engine
			partition = add_slot(self.keyNS + "matches", random.choice(range(GAME_STREAM_PARTITIONS)))
			self.redis.xadd(partition, {"match_id": match_id, "p1": p1, "p2": p2, "game": game, "state": "CREATED"})

			# Ack!
			self.redis.xack(commit_stream, GROUP_NAME, entry_id)

			pass
		elif action == "MUST_FAIL_MATCH":
			new_state = "FAILED"
			self.redis.hmset(match_key, {"p1": p1, "p2": p2, "game": game, "state": new_state})

			# Undo the lock for both players
			p1_lock = self.keyNS + "player:" + p1 + ":lock" 
			p2_lock = self.keyNS + "player:" + p2 + ":lock" 
			self.unlock(keys=[p1_lock], args=[match_id])
			self.unlock(keys=[p2_lock], args=[match_id])

			# Remove the match from each player's list
			p1_matches_key = self.keyNS + "player:" + p1 + ":matches"
			p2_matches_key = self.keyNS + "player:" + p2 + ":matches"
			self.redis.srem(p1_matches_key, match_key)
			self.redis.srem(p2_matches_key, match_key)

			# Ack!
			self.redis.xack(commit_stream, GROUP_NAME, entry_id)
		else:
			raise Exception(f"Unknown action: [{action}]")

	def process_commit_partition(self, slot, instanceID):
		stream_name = add_slot(self.keyNS + "commit", slot)
		
		# Ensure the consumer group exists
		try:
			self.redis.xgroup_create(stream_name, GROUP_NAME, id='0', mkstream=True)
		except Exception as e:
			assert e.args[0].startswith("BUSYGROUP")

		# Read any potential pending item
		streams = self.redis.xreadgroup(GROUP_NAME, instanceID, {stream_name: "0"})

		# Process items
		while True:
			for stream in streams:
				stream_name = stream[0]
				messages = stream[1]
				for (msg_id, msg) in messages:
					if "match_id" not in msg:
						msg["match_id"] = add_slot(msg_id, slot)
					self.process_match(stream_name, msg_id, msg)

			# Ask to be assigned more items
			streams = self.redis.xreadgroup(GROUP_NAME, instanceID, {stream_name: ">"}, count=10, block=6000)

	def start_processing_commit_stream(self, instanceID):
		for i in range(COMMIT_PARTITIONS):
			t = threading.Thread(target=self.process_commit_partition, args=(i, instanceID))
			t.start()


	def process_game(self, stream_name, msg_id, msg):
		match_id = msg["mid"]
		game_state = msg["state"]

		match_key = self.keyNS + "match:" + match_id
		if game_state == "READY":
			new_state = "READY"
			self.redis.hmset(match_key, {"state": new_state})
			self.redis.xack(stream_name, GROUP_NAME, msg_id)
		elif game_state == "ENDED":
			match = self.redis.hgetall(match_key)
			p1 = match["p1"]
			p2 = match["p2"]
			game = match["game"]

			new_state = "ENDED"
			self.redis.hmset(match_key, {"state": new_state})

			# Undo the lock for both players
			p1_lock = self.keyNS + "player:" + p1 + ":lock" 
			p2_lock = self.keyNS + "player:" + p2 + ":lock" 
			self.unlock(keys=[p1_lock], args=[match_id])
			self.unlock(keys=[p2_lock], args=[match_id])

			# Remove the match from each player's list
			p1_matches_key = self.keyNS + "player:" + p1 + ":matches"
			p2_matches_key = self.keyNS + "player:" + p2 + ":matches"
			self.redis.srem(p1_matches_key, match_key)
			self.redis.srem(p2_matches_key, match_key)

			# Publish the new points accruded by the players
			partition = add_slot(self.keyNS + "points", random.choice(range(POINTS_STREAM_PARTITIONS)))
			self.redis.xadd(partition, {"p1": p1, "p2": p2, "game": game, "points": 250})

			# Ack!
			self.redis.xack(stream_name, GROUP_NAME, msg_id)
		else:
			raise Exception(f"Unknown gamestate: [{action}]")

	def process_game_partition(self, stream_name, instanceID):
		# Ensure the consumer group exists
		try:
			self.redis.xgroup_create(stream_name, GROUP_NAME, id='0', mkstream=True)
		except Exception as e:
			assert e.args[0].startswith("BUSYGROUP")

		# Read any potential pending item
		streams = self.redis.xreadgroup(GROUP_NAME, instanceID, {stream_name: "0"})

		# Process items
		while True:
			for stream in streams:
				stream_name = stream[0]
				messages = stream[1]
				for (msg_id, msg) in messages:
					self.process_game(stream_name, msg_id, msg)

			# Ask to be assigned more items
			streams = self.redis.xreadgroup(GROUP_NAME, instanceID, {stream_name: ">"}, count=10, block=6000)

	def start_processing_game_streams(self, instanceID, streams: Dict[str, int]):
		for k in streams:
			for p in range(streams[k]):
				key = add_slot(k, p)
				t = threading.Thread(target=self.process_game_partition, args=(key, instanceID))
				t.start()




def add_slot(key, slot):
	return f"{key}-{{{slot}}}"
