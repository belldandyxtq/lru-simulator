#!/usr/bin/python2.7
from __future__ import division
import sys
import re
from sets import Set

KB=1000
MB=1000*KB
level1_speed=1.0
level2_speed=10.0
TOTAL_SIZE=0

class Node:
	def __init__(self,key):
		self.key=key
		self.dirty=False
		self.prev=None
		self.next=None

class DoubleLinkedList:
	def __init__(self):
		self.tail=None
		self.head=None
		self.keys={}
	def isEmpty():
		return not self.tail
	def removeLast(self):
		node=self.tail
		ret=node.dirty
		del self.keys[node.key]
		self.remove(node)
		return ret

	def remove(self,node):
		if self.head==self.tail:
			self.head,self.tail=None,None
			return
		if node == self.head:
			node.next.prev=None
			self.head=node.next
			return
		if node ==self.tail:
			node.prev.next=None
			self.tail=node.prev
			return
		node.prev.next=node.next
		node.next.prev=node.prev

	def touch(self, key, mode):
		node=self.keys[key]
		self.remove(node)
		self.addFirst(node)
		if 'w' == mode:
			node.dirty=True	

	def addFirst(self,node):
		if not self.head:
			self.head=self.tail=node
			node.prev=node.next=None
			return
		node.next=self.head
		self.head.prev=node
		self.head=node
		node.prev=None

	def insert(self, key):
		node=Node(key)
		self.addFirst(node)
		self.keys[key]=node
	
	def has(self, key):
		return key in self.keys

class buffer:
	buffer_size=0
	available_size=0
	page_size=4*KB
	swap_out_count=0
	cache_miss=0
	total_IO=0
	cache_list={}
	lru_queue=DoubleLinkedList()
	file_hash={}
	current_file_number=0
	current_page_number=0
	import_size=0
	read_size=0
	total_IO_size=0

	def __init__(self, buffer_size):
		self.buffer_size=buffer_size
		self.available_size=buffer_size
		self.swap_out_count=0
		self.total_IO=0
		self.import_size=0
		self.read_size=0
		self.total_IO_size=0

	#return file number
	def __get_file_number__(self, file_name):
		if file_name in self.file_hash:
			return self.file_hash[file_name]
		else:
			self.file_hash[file_name]=self.current_file_number
			self.current_file_number+=1
			return self.file_hash[file_name]

	def require_buffer(self, file_name, start_point, size, mode):
		file_number=self.__get_file_number__(file_name)

		tmp_start_point=start_point

		if 0 != tmp_start_point%self.page_size:
			tmp_start_point=int(tmp_start_point/self.page_size)*\
                                self.page_size

		self.total_IO_size+=size

		#split IO size into mulitple pages
		while 0 < size:
			self.total_IO+=1
			IO_size=min(self.page_size, size)
			if (file_number, tmp_start_point) in self.cache_list:
                                #if the page has gotten a number 
				#print "buffered start point %d" % (tmp_start_point)
				page_number=self.cache_list[(file_number, tmp_start_point)]
				#print "page_number %d"%page_number

				if self.lru_queue.has(page_number):
					#print "buffered in lru"
					self.__update_page__(page_number, mode)
				else:
					#print "not buffered in lru"
					self.__insert_page__(page_number, IO_size, mode)
			else:
                                #if the page is new
                                #first allocate the page number then insert
				page_number=self.__allocate_page__(file_number,\
                                        tmp_start_point, IO_size, mode)

			tmp_start_point+=IO_size
			size-=IO_size

	def __update_page__(self, page_number, mode):
		self.lru_queue.touch(page_number, mode)

	#return page number
	def __allocate_page__(self, file_number, tmp_start_point, size, mode):
		page_number=self.current_page_number
		self.current_page_number+=1
		self.cache_list[(file_number, tmp_start_point)]=page_number
                self.__insert_page__(page_number, size, mode)
		if 'r' == mode:
			self.import_size+=self.page_size

		return page_number

        def __insert_page__(self, page_number, size, mode):
		#print "available_size %d"%self.available_size
		if self.available_size >= self.page_size:
			#print "create new cache"
			self.available_size-=self.page_size
			if 'r' == mode:
				self.cache_miss+=1
		else:
			#print "swap out"
			self.__swap_out__(1)

		if 'r' == mode:
			self.read_size+=self.page_size
                self.lru_queue.insert(page_number)

	def __swap_out__(self, number):
            while 0 < number:
		if self.lru_queue.removeLast():
			#sychronized
			self.read_size+=self.page_size
		self.swap_out_count+=1
		self.cache_miss+=1
                number-=1
	
	#the total size needs transfer with remote storage
	#the value is cache_miss - clear pages
	#cache miss doesn't include write to new page
	def __level1_size__(self):
		return self.read_size

	#the total size can be served by buffer
	#the value is total IO size - cache_miss
	#cache miss doesn't include write to new page
	def __level2_size__(self):
		return self.total_IO_size - self.read_size
	
	#the speed actual can achieve with given buffer size
	def buffered_speed(self):
		level1_time=self.__level1_size__()/level1_speed
		level2_time=self.__level2_size__()/level2_speed
		total_time=level1_time+level2_time
		total_size=self.total_IO_size
		
		return float(total_size)/total_time

	#the theoretical max speed
	def max_speed(self):
		level1_time=self.import_size/level1_speed
		level2_time=(self.total_IO_size-self.import_size)/level2_speed
		total_time=level1_time+level2_time
		total_size=self.total_IO_size

		return float(total_size)/total_time

	def print_intermediate_result(self):
		sys.stdout.write("\rswap out %d, cache miss %d, total_IO %d import size %d, read size %d, total_size %d"%\
                        (self.swap_out_count, self.cache_miss, self.total_IO,\
                        self.import_size, self.read_size, self.total_IO_size))
	
	def print_final_result(self):
		#a new line
		print ""
		print "total_IO_size %f MB"%\
			(self.total_IO_size/float(MB))
		print "buffer size %f MB, page size %f MB"%\
			(self.buffer_size/float(MB), \
			self.page_size/float(MB))

		self.print_ratio()


	def print_ratio(self):
		print "ratio %f%%"%(self.buffered_speed()/self.max_speed()*100)
		#print "swap out %d, cache miss %d, total_IO %d"% \
                #        (self.swap_out_count, self.cache_miss, self.total_IO)

				
re_pattern=re.compile(r"^.+\s+(?P<time>\d+\.\d+)\s+\d+\s+(?P<type>\w)\s+(?P<path>.+)\s+(?P<offset>\d+)\s+(?P<size>\d+)\s*$")

def get_value(line, buffer):
	ret=re_pattern.match(line)
	time=0.0
	global TOTAL_SIZE

	if ret:
		re_dict=ret.groupdict()
		type=re_dict["type"]
		time+=float(re_dict["time"])
		file_name=re_dict["path"]
		if (type=="r") or (type == "w"):

			TOTAL_SIZE+=int(re_dict["size"])
			if "r" == type:
				#print 'read'
				#print file_name
				buffer.require_buffer(file_name, \
                                        int(re_dict["offset"]), \
                                        int(re_dict["size"]),\
					"r")

			elif "w" == type:
				#print 'write'
				#print file_name
				buffer.require_buffer(file_name, \
                                        int(re_dict["offset"]),\
                                        int(re_dict["size"]),\
					"w")
                        #buffer.print_intermediate_result()
	return time

def main(file, cache_size):
	cache_obj=buffer(int(cache_size)*KB)
	fd=open(file, 'r')
	time=0.0
	for line in fd:
		time+=get_value(line, cache_obj)
	fd.close()
	cache_obj.print_final_result()
if __name__ == '__main__':
	main(sys.argv[1], sys.argv[2])
