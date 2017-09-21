# smqsched
Simple Multi Queue Scheduler

**smqsched** is a simple multi-threaded task scheduling module with support for
multiple queues and directed acyclic graph (DAG) dependencies.

## Introduction

**smqsched** was designed with the following objectives in mind:

### Allow existing code to be parallelized without significant refactoring

To this end, **smqsched** uses promise-style creation of dependency graph, where
values queued to be computed in the future can be used as input to newly created
tasks in order to signify a dependency on the task producing those values.

For example, the following code:

```python
A = [numpy.random.random((100,100)) for i in range(100)]
B = [None] * 100
C = [None] * (100 ** 2)
for i in range(100):
  B[i] = numpy.linalg.inv(A[i])
    
for i in range(100):
  for k in range(100):
    C[i * 100 + k] = B[i] * B[k]
```

can be readily parallelized like this:

```python
sch = Scheduler()
Q = Queue(sch)
A = [None] * 100
B = [None] * 100
C = [None] * (100 ** 2)

for i in range(100):
  A[i] = Q.submit(numpy.random.random, (100, 100))
  B[i] = Q.submit(numpy.linalg.inv, A[i])
  
for i in range(100):
  for k in range(100):
    C[i * 100 + k] = Q.submit(lambda a, b: a * b, B[i], B[k])
    
sch.start()
sch.wait()

C = map(lambda a: a.result(), C) # get results
```

### Plan execution in order to get (partial) final results as soon as possible

**smqsched** is driving execution in such a way that the output of most recently
queued tasks (C array in the example above) are delivered as soon as possible.
Thus, the goal of **smqsched** is not simply to run as many tasks in parallel as
possible but choose tasks in such a way that the final results start coming in as
soon as possible.

For illustration purposes let's assume that the queue can only run one task at a time.
In the example above it would translate to:
- C[9999] asking for execution of B[99]
- B[99] asking for execution of A[99]
- A[99] being executed
- B[99] being executed
- C[9999] being executed
- C[9998] asking for execution of B[98]
- B[98] asking for execution of A[98]
- A[98] being executed
- B[98] being executed
- C[9998] being executed
- and so on...

Why this choice is so important will become clear in the continuation of this document.

### Support multiple queues capable of running different numbers of tasks in parallel

A typical use case of **smqsched** is a situation where the acquisition of the data needs to happen
before the processing and use of an instrument/peripheral is required to obtain the data. In such
case typically a Queue of size 1 is used for acquisition purposes and Queue of size *n_cpus* or 
*n_cpus*-1 is used for computation. Let's say that the acquired data comes in two flavors - even
and odd and the computation happens for all pairings of even/odd data. To begin obtaining outputs as
soon as possible it is necessary to acquire data in interleaved even/odd order. Thanks to **smqsched**
this fact doesn't have to be stated explicitly, the framework will ensure it happens automatically,
at the same time allowing for only one acquisition task at a time, while keeping the computation tasks
running in parallel. This is also the reason for the previous point of planning execution in such an
order that the final output starts coming in as soon as possible. It can be restated in other words -
since **smqsched** runs multiple queues it tries to keep them all full at all times and prioritizes
delivering data to most recently added tasks in case of congestion.

### Support progressive task output

**smqsched** accounts for the fact that a single task might be successively producing a number of results which
can be individually useful for dependant tasks. In other words - there might be tasks depending only on a part
of the results of another task. This observation is very important for keeping the queues full as much as possible.
Therefore **smqsched** supports generators as tasks. The following example illustrates the situation described above:

```python
def _find_minima(x):
  yield False
  for i in range(1, len(x) - 1):  
    if x[i] < x[i - 1] and x[i] < x[i + 1]:
      yield True
    else:
      yield False
  yield False
      
_find_minima.number_of_results = lambda x: len(x)

def _print_minimum(i, is_minimum):
  if is_minimum:
    print('Found minimum at', i)
    
x = np.random.random(100000)

is_minimum = Q.submit(_find_minima, x)
for i in range(len(x)):
  Q.submit(_print_minimum, i, is_minimum[i])
```

When executing this code the minima will start getting printed before they are all found.

### Support for stopping/resuming the scheduler and adding new jobs at any time

**smqsched** allows for the Scheduler to be stopped and resumed at any time. Already running
tasks cannot be interrupted unless a suitable user-implemented condition is placed in their code
however new tasks won't be started while the scheduler is stopped. Resuming the scheduler will
pick up the work without losing any data or the need to re-run any of the already completed tasks.
This functionality is of particular importance if tasks have side-effects, particularly on the I/O
side, for example temporary files which might be useful for inspection by the developer. New jobs
can be added to the queues while the scheduler is already running. This allows to use **smqsched**
in a more interactive manner and new dependencies will be taken into account as soon as the next
scheduling cycle occurs.

## Final Thoughts

**smqsched** can be used in multiple situations involving data acquisition, network communication
as well as pure computation. It currently supports thread-based parallelism but can be easily
adapted to support process-based or cluster-based setups.

## License

**smqsched** is licensed under 2-clause "Simplified" BSD License.
