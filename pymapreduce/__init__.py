#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import multiprocessing, sys, cPickle, os, datetime
from multiprocessing.queues import SimpleQueue
from tempfile import NamedTemporaryFile, TemporaryFile
import heapq, itertools

class Job(object):
    """
    Base class of the Job.
    
    job.enumerate() should return an interator over ITEMS
    
    for each INPUT
        job.map(item, cb) is called 
        cb should be called for each key-value pair
        
    all key values pairs are sorted, and for each key
        job.reduce_key_start(key) is called
        job.reduce_value(value) is called
        job.reduce_key_stop(key) is called        
    """
        
    def map(self, item, cb):
        cb (item)
    
    def reduce_start(self):
        pass
        
    def reduce_key_start(self, key):
        pass
        
    def reduce_key_stop(self, key):
        pass
        
    def reduce_value(self, r):
        pass
        
    def reduce_stop(self):
        pass
        
class WC(Job):
    "Sample Word count parallel implementation"
    lc = 0
    wc = 0 
    bc = 0
    def __init__(self, f):
        self.file = f
    
    def reduce_start(self):
        self.lc = 0
        self.wc = 0
        self.bc = 0 
        
    def enumerate(self):
        return enumerate(open(self.file))
        
    def map(self, item, cb):
        (pos, line) = item
        cb((pos, (1, len(line.split()), len(line))))
        
    def reduce_value(self, r):
        (lc, wc, bc) = r
        self.lc = self.lc + lc
        self.wc = self.wc + wc
        self.bc = self.bc + bc
                
    def reduce_stop(self):
        return (self.lc, self.wc, self.bc)

def debug_print(s):
    print >> sys.stderr, "[%s] (pid %u) %s" % (datetime.datetime.now().strftime('%H:%M:%S'),  os.getpid(), s)

class BaseRunner(object):
    STOP_MSG = "##STOP_MSG##"
    def __init__(self):
        self.debug = False
        pass
    def reduce_loop(self, item_iterator):
        job = self.job
        job.reduce_start()
        pkey = None
        for (key, val) in item_iterator:
            if pkey == None or pkey != key:
                if not (pkey is None):
                    job.reduce_key_stop(pkey)
                job.reduce_key_start(key)
                pkey = key
            job.reduce_value(val)
        if not (pkey is None):
            job.reduce_key_stop(pkey)
        return job.reduce_stop()
       
class SingleThreadRunner(BaseRunner):
    """
    Runner that executes a job in a single thread on a single process
    """
    def __init__(self):
        pass
    def run(self, job):
        self.job = job 
        buf = []
        for elt in job.enumerate(): 
            job.map(elt, buf.append)
        buf.sort()
        return self.reduce_loop(buf)
        
class BaseMultiprocessingRunner(BaseRunner):
    def __init__(self):
        super(BaseMultiprocessingRunner, self).__init__()
        self.numprocs = max(multiprocessing.cpu_count() - 1, 1)
        self.map_input_queue = SimpleQueue()
        self.map_output_queue = SimpleQueue()
    def run_map(self):
          for item in iter(self.map_input_queue.get, self.STOP_MSG):
              self.job.map(item, self.map_output_queue.put)
          self.map_output_queue.put(self.STOP_MSG)
          if self.debug:
              debug_print("Output : STOP sent")        
    def run_enumerate(self):
        for inp in self.job.enumerate(): 
            self.map_input_queue.put(inp)
        for work in range(self.numprocs):
            self.map_input_queue.put(self.STOP_MSG)
        if self.debug: 
            debug_print("Input: STOP sent")         
    def run(self, job):
        self.job = job 
        # Process that reads the input file
        self.enumeration_process = multiprocessing.Process(target=self.run_enumerate, args=())
        
        self.mappers = [ multiprocessing.Process(target=self.run_map, args=())
                        for i in range(self.numprocs)]
                     
        self.enumeration_process.start()
        for mapper in self.mappers:
            mapper.start()
        r = self.run_reduce()
        self.enumeration_process.join()
        for mapper in self.mappers:
            mapper.join()
        return r 

class DiskBasedRunner(BaseMultiprocessingRunner):
    def __init__(self, map_buffer_size = 10000, reduce_max_files = 10 ):
        super(DiskBasedRunner, self).__init__()
        self.item_buffer = {}
        self.map_buffer_size = map_buffer_size
        self.reduce_max_files = reduce_max_files
        self.map_opened_files = []
        
    def run_map(self):
        self.item_buffer = []
        for item in iter(self.map_input_queue.get, self.STOP_MSG):
            self.job.map(item, self.item_buffer.append)
            if len(self.item_buffer) > self.map_buffer_size: 
                self.map_buffer_clear()
        self.map_buffer_clear()
        self.map_output_queue.put(self.STOP_MSG)
        if self.debug:
            debug_print("Map done")
        
    def map_buffer_clear(self):
        self.item_buffer.sort()
        f = NamedTemporaryFile() # We keep the file opened as it would close automatically 
        if self.debug:
            debug_print('Temp file %s' % f.name)
        for item in self.item_buffer:
            cPickle.dump(item, f, cPickle.HIGHEST_PROTOCOL)
        f.flush()
        self.map_opened_files.append(f)
        self.map_output_queue.put(f.name)
        del self.item_buffer[:]
        
    def get_next_file(self):
        while self.stopped_received < self.numprocs:
            filename = self.map_output_queue.get()
            if filename == self.STOP_MSG:
                self.stopped_received = self.stopped_received + 1 
                if self.debug:
                    debug_print("Reduced received complete output from %u mappers" % self.stopped_received)
                continue
            else: 
                if self.debug: 
                    debug_print('Reading %s' % filename)
                yield open(filename, 'r')
        if self.debug:
            debug_print('All files from mappers received')
        
    def iter_on_file(self, stream):
        try:
            while True:
                yield cPickle.load(stream)
        except EOFError:
            stream.close()
            if hasattr(stream, "name"): 
                os.remove(stream.name) 
            
    def run_reduce(self):
        self.stopped_received = 0 
        self.merged_files = []
        merged_iterator = None
        while True:
            # Iterate and merge files until all jobs are processed
            get_next = self.get_next_file()
            files = get_next 
            #itertools.islice(get_next, self.reduce_max_files)
            all_files = [file for file in files]
            iterables = [self.iter_on_file(file) for file in all_files]
            merged_iterator = heapq.merge(*iterables)
            if self.stopped_received < self.numprocs: 
                if self.debug: 
                    debug_print("Performing intermediate merge on %u  files" % len(iterables))
                f = TemporaryFile()
                self.merged_files.append(f)
                for m in merged_iterator:
                    cPickle.dump(m, f, cPickle.HIGHEST_PROTOCOL)
                f.seek(0)
                f.flush()
            else:
                break
        if len(self.merged_files) > 0:
            if self.debug:
                debug_print("Final merge")
            # Final merge if required
            merged_iterator = heapq.merge(*([self.iter_on_file(stream) for stream in self.merged_files]+[merged_iterator]))
        if self.debug:
            debug_print("Reduce loop")
        result = self.reduce_loop(merged_iterator) 
        return result 
            
class SedLikeJobRunner(BaseMultiprocessingRunner):
    """
    Runner optimzed for jobs that outputs key values of the form (i, value) where i are consecutive integer
    starting at '0'
    """
    def __init__(self):
        super(SedLikeJobRunner, self).__init__()
    def run_reduce(self):
        cur = 0
        buffer = {}
        self.job.reduce_start()
        for mappers in range(self.numprocs):
            for msg in iter(self.map_output_queue.get, self.STOP_MSG):
                (i, val)  = msg 
                # verify rows are in order, if not save in buffer
                if i != cur:
                    buffer[i] = val
                else:
                    self.job.reduce_key_start(i)
                    self.job.reduce_value(val)
                    self.job.reduce_key_stop(i)
                    cur += 1 
                    while cur in buffer:
                        self.job.reduce_key_start(cur)
                        self.job.reduce_value(buffer[cur])
                        self.job.reduce_key_stop(cur)
                        del buffer[cur]
                        cur += 1
            if self.debug:
                debug_print("Mapper done %u" % mappers)
        return self.job.reduce_stop()
    
class WCLikeJobRunner(BaseMultiprocessingRunner):
    """
    Runner optimized for jobs that outputs always the same key, and perform only a global reduce over all values
    """
    def __init__(self):
        super(WCLikeJobRunner, self).__init__()
        
    def run_reduce(self):
        self.job.reduce_start()
        for mappers in range(self.numprocs):
            for msg in iter(self.map_output_queue.get, self.STOP_MSG):
                (key, val)  = msg 
                self.job.reduce_value(val)
        return self.job.reduce_stop()
        
class RambasedRunner(BaseMultiprocessingRunner):
    def __init__(self):
        super(RambasedRunner, self).__init__()

    def run_reduce(self):
        self.job.reduce_start()
        buf = []
        for mappers in range(self.numprocs):
            for msg in iter(self.map_output_queue.get, self.STOP_MSG): 
                buf.append(msg)
        buf.sort()
        return self.reduce_loop(buf)
        
if __name__ == "__main__":
    runners = []
    runners.append(SingleThreadRunner())
    runners.append(RambasedRunner())
    runners.append(WCLikeJobRunner())
    runners.append(SedLikeJobRunner())
    runners.append(DiskBasedRunner())
    for runner in runners:
        runner.debug = True
        for argv in sys.argv[1:]:
            (lc, wc, bc) = runner.run(WC(argv))
            print "(%s)\t%u\t%u\t%u\t%s" % (runner.__class__.__name__, lc, wc, bc, argv)
