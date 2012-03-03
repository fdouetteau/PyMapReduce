

Py MapReduce
=================

Py MapReduce is a simple monoserver implementation of MapReduce in python, using the multiprocessing module. 
It can be use for instance for quick parallelization of file processing task, e.g. performing operations on each line of a large file.  
Simple operations (regexp matching etc..) are hard to multithread in python because of the Global Interpreter Lock (http://wiki.python.org/moin/GlobalInterpreterLock). Here multiprocessing can help
 
Sample job (Word Count)
------------

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
        
	    def map(self, pos, item):
	        return (pos, (1, len(item.split()), len(item)))
        
	    def reduce(self, pos, r):
	        (lc, wc, bc) = r
	        self.lc = self.lc + lc
	        self.wc = self.wc + wc
	        self.bc = self.bc + bc
                
	    def reduce_stop(self):
	        return (self.lc, self.wc, self.bc)