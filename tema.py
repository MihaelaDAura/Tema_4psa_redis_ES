import redis
import random
from threading import Thread
import time
from threading import Lock

redis_db = redis.StrictRedis(host="localhost", port=6379, db=0)
lock_fibonnaci = Lock()
lock_heartbeats = Lock()

def fibonnaci(x):
    a = 0
    b = 1
    while a + b < x:
        aux = b
        b = a + b
        a = aux
    return b + x

def work(worker):
    while not worker.needs_to_quit:
        with lock_fibonnaci:
            current_value = int(redis_db.get("fib"))
        next_value = fibonnaci(current_value)
        with lock_fibonnaci:
            redis_db.set("fib", next_value)
        time.sleep(worker.w)

def crash(worker):
    while not worker.needs_to_quit:
        if random.randint(1, 10) == 1:
            worker.quit_worker()
            break
        time.sleep(worker.c)

def heartbeat(worker):
    while not worker.needs_to_quit:
        worker.score = int(time.time())
        my_heartbeat = {}
        my_heartbeat[worker.member] = worker.score
        with lock_heartbeats:
            redis_db.zadd("heartbeats", my_heartbeat)
        time.sleep(worker.t)

class Worker:
    def __init__(self, member, t, w, c, n):
        self.member = member
        self.t = t
        self.w = w
        self.c = c
        self.n = n
        self.needs_to_quit = False
        with lock_heartbeats:
            oldest_worker_string = redis_db.zrange("heartbeats", 0, 0)
        if oldest_worker_string != []:
            oldest_worker_list = str(oldest_worker_string[0]).split("\'")
            oldest_worker_member = oldest_worker_list[1]
            with lock_heartbeats:
                oldest_worker_score = redis_db.zscore("heartbeats", oldest_worker_member)
            self.score = int(time.time())
            while oldest_worker_score < self.score - self.t:
                with lock_heartbeats:
                    redis_db.zrem("heartbeats", oldest_worker_member)
                with lock_heartbeats:
                    oldest_worker_string = redis_db.zrange("heartbeats", 0, 0)
                if not oldest_worker_string:
                    break
                oldest_worker_list = str(oldest_worker_string[0]).split("\'")
                oldest_worker_member = oldest_worker_list[1]
                with lock_heartbeats:
                    oldest_worker_score = redis_db.zscore("heartbeats", oldest_worker_member)
                self.score = int(time.time())
        with lock_heartbeats:
            elements_in_heartbeat = redis_db.zcard("heartbeats")
        if elements_in_heartbeat < self.n:
            self.heartbeat_thread = Thread(target=heartbeat, args=(self,))
            self.work_thread = Thread(target=work, args=(self,))
            self.crash_thread = Thread(target=crash, args=(self,))
            self.heartbeat_thread.start()
            self.work_thread.start()
            self.crash_thread.start()

    def quit_worker(self):
        self.needs_to_quit = True
        self.work_thread.join()
        self.heartbeat_thread.join()

def manager(m, t, w, c, n):
    id = 0
    with lock_fibonnaci:
        redis_db.set("fib", "1")
    while True:
        Worker(id, t, w, c, n)
        id = id + 1
        time.sleep(m)        

def print_for_checking():
    i = 0
    while i < 80:
        with lock_heartbeats:
            print("We have " + str(redis_db.zcard("heartbeats")) + " workers")
        with lock_fibonnaci:
            print("We have " + str(redis_db.get("fib")) + " as fibbonaci")
        time.sleep(0.5)
        i = i + 1
if __name__ == "__main__":
    #am impartit M, T, W, C la 100 pentru dura foarte mult
    #pana se creau threadurile
    manager_thread = Thread(target=manager, args=(0.3, 0.1, 0.2, 0.1, 5))
    manager_thread.start()
    checking = Thread(target=print_for_checking)
    checking.start()
    checking.join()