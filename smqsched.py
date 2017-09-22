#
# Copyright (C) Stanislaw Adaszewski, 2017
#

from threading import Thread, RLock, Condition
from concurrent.futures import ThreadPoolExecutor
import os
import time
import numpy as np
from collections import Iterable
from multiprocessing import cpu_count


_WAITING = 0
_RUNNING = 1
_FINISHED = 2
_FAILED = 3


class Scheduler(object):
	def __init__(self):
		self._q = []
		self._cond = Condition()
		self._queues = []
		self._keep_running = False
		self._thr = None
		
	def _try_start_job(self, job):
		if job._status == _WAITING:	
			deps_ready = all(map(lambda par: \
				par._status == _FINISHED, \
				job._parents))
			if deps_ready:
				if job._queue.has_resources(job):
					job._queue.start(job)
				return
				
			for par in job._parents:
				self._try_start_job(par)
				
	def _try_start(self):
		for job in reversed(self._q):
			self._try_start_job(job)
		
	def add_job(self, job):
		with self._cond:
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
				if all_done:
					break
					
				self._try_start()
				self._cond.wait()
		
	def start(self):
		self._thr = Thread(target=self._start)
		self._thr.start()
			
	def stop(self):
		with self._cond:
			self._keep_running = False
			self._cond.notify()
			
	def wait(self):
		self._thr.join()
		
	def is_running(self):
		return self._thr.isAlive()
		
		
def _mklist(a):
	if isinstance(a, Iterable):
		return a
	return [a]
		
		
class Job(object):
	def __init__(self, queue, runnable, *args, **kwargs):
		self._queue = queue
		self._parents = []
		self._children = []
		self._runnable = runnable
		self._args = args
		self._status = _WAITING
		self._result = None
		self._promise = None
		self._sub_jobs = None
		for a in args:
			if isinstance(a, Job):
				self.add_dependency(a)
		self._resources = (kwargs['resources'] if 'resources' in kwargs else {})
		self._hold = (kwargs['hold'] if 'hold' in kwargs else False)
		self._release = _mklist(kwargs['release'] if 'release' in kwargs else [])
		for rj in self._release:
			self.add_dependency(rj)
		if 'after' in kwargs:
			for a in _mklist(kwargs['after']):
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
	def __init__(self, sch, **kwargs):
		self._max_workers = kwargs['max_workers'] \
			if 'max_workers' in kwargs else cpu_count()
		self._sch = sch
		sch._queues.append(self)
		self._used_workers = 0
		self._resources = kwargs['resources'] \
			if 'resources' in kwargs else {}
		self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
				
	def submit(self, runnable, *args, **kwargs):
		job = Job(self, runnable, *args, **kwargs)
		self._sch.add_job(job)
		
		if hasattr(runnable, 'number_of_results'): # multiple results job
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
			cond = self._sch._cond
			
			if job._sub_jobs is not None: # multiple result job
				gen = job._runnable(*args)
				for i in range(len(job._sub_jobs)):
					try:
						res = next(gen)
						sta = _FINISHED
					except Exception as e:
						print(e)
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
				except Exception as e:
					print(e)
					res = None
					sta = _FAILED
			
			with cond:
				for rj in job._release:
					rj._queue.free_resources(rj)
				if not job._hold:
					self.free_resources(job)
				job._status = sta
				job._result = res
				cond.notify()
			
			return res
			
		return wrapped
		
	def start(self, job):
		with self._sch._cond:
			if not self.has_resources(job):
				raise ValueError('Attempting to start a job on a queue without necessary resources')
			job._status = _RUNNING
			self.take_resources(job)
			job._promise = self._executor.submit(self.wrap(job), *job.extract_args())
				
	def has_resources(self, job):
		if self._used_workers >= self._max_workers:
			return False
			
		for (k, v) in job._resources.items():
			if self._resources[k] < v:
				return False
				
		return True
		
	def take_resources(self, job):
		self._used_workers += 1
		for (k, v) in job._resources.items():
			self._resources[k] -= v
			
	def free_resources(self, job):
		self._used_workers -= 1
		for (k, v) in job._resources.items():
			self._resources[k] += v
		
		
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
	acq = Queue(sch, max_workers=1)
	prq = Queue(sch)

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
