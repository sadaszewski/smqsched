#
# Copyright (C) Stanislaw Adaszewski, 2017
#

from threading import Thread, RLock, Condition
# from concurrent.futures import ThreadPoolExecutor, \
	# ProcessPoolExecutor
import os
import time
import numpy as np


_WAITING = 0
_RUNNING = 1
_FINISHED = 2
_FAILED = 3


class Scheduler(object):
	def __init__(self):
		self._q = []
		self._lock = RLock()
		self._cond = Condition(self._lock)
		self._queues = []
		self._keep_running = False
		self._thr = None
		
	def _try_start_job(self, job):
		if job._status == _WAITING:	
			# print('job._parents:', job._parents)
			deps_ready = all(map(lambda par: \
				par._status == _FINISHED, \
				job._parents))
			# print('deps_ready:', deps_ready)
			if deps_ready:
				if job._queue.has_free_workers():
					job._queue.start(job)
				return
				
			for par in job._parents:
				self._try_start_job(par)
				
	def _try_start(self):
		print('try_start(): ENTER')
		# t_1 = time.time()
		# print('elapsed 1:', time.time() - t_1)
		for job in reversed(self._q):
			self._try_start_job(job)
		print('try_start(): EXIT')
		
	def add_job(self, job):
		t_1 = time.time()
		with self._lock:
			# print('elapsed 2:', time.time() - t_1)
			self._q.append(job)

	def _start(self):
		self._keep_running = True
		with self._cond:
			while self._keep_running:
				any_failed = any(map(lambda job: \
					job._status == _FAILED, \
					self._q))
				if any_failed:
					raise ValueError('Some jobs failed, aborting...')
				
				all_done = all(map(lambda job: \
					job._status == _FINISHED, \
					self._q))
				# print('all_done:', all_done)
				if all_done:
					break
					
				self._try_start()
				for queue in self._queues:
					print('max_workers: %d, used_workers: %d' % \
						(queue._max_workers, queue._used_workers))
				t_1 = time.time()
				# return
				self._cond.wait()
				print('elapsed waiting for any job to complete:', time.time() - t_1)
		
	def start(self):
		print('Starting...')
		self._thr = Thread(target=self._start)
		self._thr.start()
		# try:
			# self._start()
		# except KeyboardInterrupt:
			# print('Got kbd interrupt, shutting down...')
			# os._exit(-1)
			
	def stop(self):
		with self._cond:
			print('Stop requested...')
			self._keep_running = False
			self._cond.notify()
			
	def wait(self):
		self._thr.join()
		
	def is_running(self):
		return self._thr.isAlive()
		
		
class Job(object):
	def __init__(self, queue, runnable, *args):
		# print('Job(), args:', args)
		self._queue = queue
		self._parents = []
		self._children = []
		self._runnable = runnable
		self._args = args
		self._status = _WAITING
		self._result = None
		# self._promise = None
		self._sub_jobs = None
		for a in args:
			if isinstance(a, Job):
				self.add_dependency(a)
		
	def add_dependency(self, par):
		par._children.append(self)
		self._parents.append(par)
		
	def extract_args(self):
		args = []
		for a in self._args:
			if isinstance(a, Job):
				args.append(a.result())
			else:
				args.append(a)
		return args
		
	def result(self):
		return self._result
		
		
class Queue(object):
	def __init__(self, sch):
		self._sch = sch
		sch._queues.append(self)
		self._used_workers = 0
		
	def has_free_workers(self):
		with self._sch._lock:
			return (self._used_workers < self._max_workers)
		
	def submit(self, runnable, *args):
		job = Job(self, runnable, *args)
		self._sch.add_job(job)
		
		if 'number_of_results' in runnable.__dict__: # multiple results job
			n = runnable.number_of_results(*args)
			sub_jobs = [None] * n
			for i in range(n): # generator will be called n times
				sub_jobs[i] = Job(self, None) # doesn't have to execute anything
				sub_jobs[i].add_dependency(job)
				self._sch.add_job(sub_jobs[i])
			job._sub_jobs = sub_jobs
			return sub_jobs
		else:
			return job
		
	def wrap(self, job):
		def wrapped(*args):
			print('wrapped(): ENTER, args:', args)
			cond = self._sch._cond
			
			if job._sub_jobs is not None: # multiple result job
				gen = job._runnable(*args)
				for i in range(len(job._sub_jobs)):
					try:
						res = next(gen)
						sta = _FINISHED
					except:
						res = None
						sta = _FAILED
				
					with cond:
						job._sub_jobs[i]._result = res
						job._sub_jobs[i]._status = sta
						cond.notify()
				res = None
				sta = _FINISHED
			else:
				try:
					res = job._runnable(*args)
					sta = _FINISHED
				except:
					res = None
					sta = _FAILED
			
			t_1 = time.time()
			with cond:
				elapsed = time.time() - t_1
				print('elapsed:', elapsed)
				self._used_workers -= 1
				job._status = sta
				job._result = res
				cond.notify()
			
			print('wrapped(): EXIT')
			return res
		return wrapped
		
	def start(self, job):
		t_1 = time.time()
		with self._sch._lock:
			print('elapsed 3:', time.time() - t_1)
			if self._used_workers >= self._max_workers:
				raise ValueError('Attempting to start a job on full queue')
			job._status = _RUNNING
			self._used_workers += 1
			t_1 = time.time()
			job._promise = self._executor.submit(self.wrap(job), *job.extract_args())
			print('elapsed 4:', time.time() - t_1)
		
		
class Executor(object):
	def __init__(self):
		pass
		
	def submit(self, runnable, *args):
		thr = Thread(target=runnable, args=args)
		thr.start()
		
		
class AcquisitionQueue(Queue):
	def __init__(self, sch):
		super(AcquisitionQueue, self).__init__(sch)
		self._max_workers = 1
		self._executor = Executor()
		
	def hline(pos, width=0):
		pass
		
	def vline(pos, width=0):
		pass
		
		
class ProcessingQueue(Queue):
	def __init__(self, sch):
		super(ProcessingQueue, self).__init__(sch)
		self._max_workers = 8
		self._executor = Executor()

		
def _compute_square(i):
	print('_compute_square(), i:', i)
	# np.random.random((10000,10000))
	res = i ** 2
	# print('res:', res)
	return res
	
		
def _compute_sum(i, k, a, b, c, d, E):
	print('_compute_sum(), i:', i, 'k:', k)
	# A = np.random.random((1000,1000))
	X = (E * E) + (E * E * E) # np.linalg.inv(E)
	# raise ValueError('dummy')
	res = a + b + c + d
	print('---> res:', res)
	return res
		
# print = lambda *args: 0
def _duplicate(a):
	print('_duplicate(), 1')
	yield a
	print('_duplicate(), 2')
	yield a
	yield a
	
_duplicate.number_of_results = lambda a: 3
		
def main(): # a bunch of simple tests

	sch = Scheduler()
	acq = AcquisitionQueue(sch)
	prq = ProcessingQueue(sch)

	n = 10
	squares = [None] * n
	for i in range(n):
		squares[i] = acq.submit(_compute_square, i)
		
	duplicates = [None] * n
	for i in range(n):
		duplicates[i] = prq.submit(_duplicate, squares[i])
		
	# for i in range(n):
		# squares[i].result(0)
		
	sums = [None] * (n ** 2)
	E = np.random.random((7000,7000))
	for i in range(n):
		for k in range(n):
			sums[i * n + k] = prq.submit(_compute_sum, \
				i, k, duplicates[i][0], duplicates[i][1], \
				duplicates[k][0], duplicates[k][2], E)
		
	sch.start()
	time.sleep(10)
	print('Doing a premature stop...')
	sch.stop()
	time.sleep(10)
	print('Ok now resuming...')
	sch.start()
	try:
		while sch.is_running():
			time.sleep(1)
	except KeyboardInterrupt:
		print('Got kbd interrupt, stopping...')
		sch.stop()
	sch.wait()
	
	print('Done')
	
	# verify
	for i in range(n):
		for k in range(n):
			assert sums[i * n + k].result() == 2 * ((i ** 2) + (k ** 2))
	print('Verify OK')
		
if __name__ == '__main__':
	main()
