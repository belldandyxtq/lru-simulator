#!/usr/bin/python2.7
from __future__ import division
import sys
import re
from sets import Set
from Queue import Queue

KB=1000
MB=1000*KB
level1_speed=1000.0*MB
speed_diff=6.0
level2_speed=speed_diff*level1_speed
factor=1.0-(1.0/10.0)

def print_address(address, size, mode):
	print address, size, mode

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
		#print "key %d"%(node.key)
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
		#whether the page is dirty before write
		ret=False
		node=self.keys[key]
		self.remove(node)
		self.addFirst(node)
		if 'w' == mode:
			#print "key %d dirty"%(key)
			#the page was dirty before	
			#and to be writtten
			if node.dirty:
				ret=True
			node.dirty=True	
		return node,ret

	def addFirst(self,node):
		if not self.head:
			self.head=self.tail=node
			node.prev=node.next=None
			return
		node.next=self.head
		self.head.prev=node
		self.head=node
		node.prev=None

	def insert(self, key, mode):
		node=Node(key)
		self.addFirst(node)
		self.keys[key]=node
		if 'w' == mode:
			node.dirty=True
		return node
	
	def has(self, key):
		return key in self.keys

class buffer:
	buffer_size=0
	available_size=0
	page_size=4*KB

	cache_list={}
	lru_queue=DoubleLinkedList()
	write_back_queue=Queue()
	file_hash={}
	file_size_suffix={}

	current_file_number=0	#the file number counter, used to assign
				#file number
	current_page_number=0	#the page number counter, used to assign
				#page number

	level1_size=0		#the total size goes to level1 storage
	total_IO_size=0		#the total IO size

	previous_sec=0		#sec timestamp of previous op
	previous_usec=0		#usec timestamp of previous op
	previous_remain=0	#the write back data remains from previous
				#write back, the value should be small than
				#one page

	swap_out_count=0	#the total number of swap out
	cache_miss=0		#the total number of cache miss
	total_IO=0		#the total IO pages
	rewritten_page=0	#number of page to be rewritten
	write_size=0 		#total write size
	read_size=0		#total read size 
	re_read_size=0 		#buffered size
	overwrite_size=0 	#buffered size
	firstwrite_size=0 	#buffered size
	write_back_size=0 	#unbuffered size
	import_size=0 		#unbuffered size
	swapin_read_size=0	#unbuffered size

	def __init__(self, buffer_size):
		self.buffer_size=buffer_size
		self.available_size=buffer_size
		self.swap_out_count=0
		self.total_IO=0
		self.import_size=0
		self.total_IO_size=0

	def get_time_diff(self, time_sec, time_usec):
		if 0 == self.previous_sec:
			self.previous_sec=time_sec
			self.previous_usec=time_usec
		diff=time_sec-self.previous_sec
		diff+=(time_usec-self.previous_usec)*0.001*0.001
		self.previous_sec=time_sec
		self.previous_usec=time_usec
		return diff

	def update_file_size_suffix(self):
		total_size=0
		for (file_number, file_size) in self.file_size_suffix.items():
			tmp_size=total_size+file_size
			file_size=total_size
			total_size=tmp_size

	def set_max_size(self, file_path, max_size):
		if not file_path in self.file_hash:
			self.file_hash[file_path]=self.current_file_number
			self.file_size_suffix[self.current_file_number]=max_size
			self.current_file_number+=1
		else:
			file_number=self.file_hash[file_path]
			origin_size=self.file_size_suffix[file_number]
			self.file_size_suffix[file_number]=\
			max(max_size, origin_size)

	#return file number
	def __get_file_number__(self, file_name):
		if file_name in self.file_hash:
			return self.file_hash[file_name]
		else:
			number=self.current_file_number
			self.file_hash[file_name]=number
			self.file_size_suffix[number]=0
			self.current_file_number+=1
			return number

	def update_write_back_size(self, time_diff):
		#the size can be written back during the time diff	
		written_back_size=time_diff*level1_speed+self.previous_remain

		while self.page_size <= written_back_size:
			if not self.write_back_queue.empty():
				#print "write back %d"%(written_back_size)
				node=self.write_back_queue.get()
				node.dirty=False
				written_back_size-=self.page_size
			else:
				break
		if not self.write_back_queue.empty():
			#print "cannot write back %d"%(written_back_size)
			self.previous_remain=max(0, written_back_size)
		else:
			self.previous_remain=0

	def require_buffer(self, file_name,\
			tv_sec, tv_usec,\
			duration,\
			start_point, size, mode):
		file_number=self.__get_file_number__(file_name)

		time_diff=self.get_time_diff(tv_sec, tv_usec)-duration*factor
		if 0.0 > time_diff:
			time_diff=0.0

		#print "time diff %f"%(time_diff)
		#Asychronized
		self.update_write_back_size(time_diff)
		#end

		self.total_IO_size+=size
		
		if 'w' == mode:
			self.write_size+=size
		else:
			self.read_size+=size

		tmp_start_point=start_point

		if 0 != tmp_start_point%self.page_size:
			tmp_start_point=int(tmp_start_point/self.page_size)*\
                                self.page_size
			size+=tmp_start_point-start_point

		#split IO size into mulitple pages
		while 0 < size:
			self.total_IO+=1
			IO_size=min(self.page_size, size)

			self.__handle_request__(file_number,\
						tmp_start_point,\
						IO_size, mode)

			tmp_start_point+=IO_size
			size-=IO_size

	def __handle_request__(self, file_number, start_point, size, mode):
		if (file_number, start_point) in self.cache_list:
                        #if the page has gotten a number 
			#print "buffered start point %d" % (tmp_start_point)
			page_number=self.cache_list[(file_number, start_point)]
			#print "page_number %d"%page_number

			if self.lru_queue.has(page_number):
				#print "buffered in lru"
				self.__update_page__(page_number, mode, size)
				print_address(self.get_file_address(file_number,\
						start_point),\
						size, 'b')
						
			else:
				#print "not buffered in lru"
				self.__insert_page__(page_number, size, mode)
				print_address(self.get_file_address(file_number,\
						start_point),\
						size, 'u')
				if 'r' == mode:
					self.swapin_read_size+=size

		else:
			#if the page is new
			#first allocate the page number then insert
			page_number=self.__allocate_page__(file_number,\
                                   start_point, size, mode)
			print_address(self.get_file_address(file_number,\
					start_point),\
					size, 'u')

	def get_file_address(self, file_number, start_point):
		return self.file_size_suffix[file_number]+start_point

	def __update_page__(self, page_number, mode, size):
		node, ret=self.lru_queue.touch(page_number, mode)
		if 'w' == mode:
			self.overwrite_size+=size
		else:
			self.re_read_size+=size
		#Asychronized
		if 'w' == mode:
			self.write_back_queue.put(node)
		#end

	#return page number
	def __allocate_page__(self, file_number, tmp_start_point, size, mode):
		page_number=self.current_page_number
		self.current_page_number+=1
		self.cache_list[(file_number, tmp_start_point)]=page_number
                self.__insert_page__(page_number, size, mode)
		if 'r' == mode:
			self.import_size+=self.page_size
		else:
			self.firstwrite_size+=size

		return page_number

        def __insert_page__(self, page_number, size, mode):
		#print "available_size %d"%self.available_size
		if self.available_size >= self.page_size:
			#print "create new cache"
			#print "available_size %d"%(self.available_size)
			self.available_size-=self.page_size
			if 'r' == mode:
				self.cache_miss+=1
		else:
			#print "swap out"
			self.__swap_out__(1)
		node=self.lru_queue.insert(page_number, mode)

		if 'r' == mode:
			self.level1_size+=self.page_size
		#Asychronized
		else:
			self.write_back_queue.put(node)
		#end

	def __swap_out__(self, number):
            while 0 < number:
		if self.lru_queue.removeLast():
			#sychronized
			#write back page
			#print "buffer overflow"
			self.write_back_size+=self.page_size
			self.level1_size+=self.page_size
		self.swap_out_count+=1
		self.cache_miss+=1
                number-=1
	
	#the total size needs transfer with remote storage
	#the value is cache_miss - clear pages
	#cache miss doesn't include write to new page
	def __level1_size__(self):
		return self.level1_size

	#the total size can be served by buffer
	#the value is total IO size - cache_miss
	#cache miss doesn't include write to new page
	def __level2_size__(self):
		return self.total_IO_size
	
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
		#sys.stdout.write("\rswap out %d, cache miss %d, total_IO %d import size %d, level1 size %d, total_size %d"%\
		print ("swap out %d, cache miss %d, total_IO %d import size %d, level1 size %d, total_size %d, write_size %d, read_size %d, rewritten page ratio %f"%\
                        (self.swap_out_count, self.cache_miss, self.total_IO,\
                        self.import_size, self.level1_size, self.total_IO_size,\
			self.write_size, self.read_size,\
			self.rewritten_page/self.total_IO_size))
		print ("write size %d, re-read size %d, write back size %d, import size %d, swapin_read_size %d"%\
				(self.write_size, self.re_read_size, \
				self.write_back_size, self.import_size,\
				self.swapin_read_size))
	
	def print_final_result(self):
		#a new line
		print ""
		print "total_IO_size %f MB"%\
			(self.total_IO_size/float(MB))
		print "buffer size %f MB, page size %f MB"%\
			(self.buffer_size/float(MB), \
			self.page_size/float(MB))

		self.print_intermediate_result()
		self.print_ratio()


	def print_ratio(self):
		print "max througput ratio %f%%"%(self.max_speed()/level2_speed*100)
		print "actual througput ratio %f%%"%(self.buffered_speed()/level2_speed*100)
		print "ratio %f%%"%(self.buffered_speed()/self.max_speed()*100)
		#print "swap out %d, cache miss %d, total_IO %d"% \
                #        (self.swap_out_count, self.cache_miss, self.total_IO)

				
re_pattern=re.compile(r"^(?P<time_sec>\d+)\.(?P<time_usec>\d+)\s+(?P<duration>\d+\.\d+)\s+\d+\s+(?P<type>\w)\s+(?P<path>.+)\s+(?P<offset>\d+)\s+(?P<size>\d+)\s*$")

def get_max_size(line, buffer):
	ret=re_pattern.match(line)
	if ret:
		ret_dic=ret.groupdict()
		file_path=ret_dic['path']
		max_size=int(ret_dic['offset'])+int(ret_dic['size'])
		buffer.set_max_size(file_path, max_size)

def get_value(line, buffer):
	ret=re_pattern.match(line)
	time=0.0
	if ret:
		re_dict=ret.groupdict()
		type=re_dict["type"]
		time+=float(re_dict["duration"])
		file_name=re_dict["path"]
		if (type=="r") or (type == "w"):

			if 'r' == type:
				#print 'read'
				#print file_name
				buffer.require_buffer(file_name, \
					int(re_dict["time_sec"]),\
					int(re_dict["time_usec"]),\
					float(re_dict["duration"]),\
                                        int(re_dict["offset"]), \
                                        int(re_dict["size"]),\
					"r")
			elif 'w' == type:
				#print 'write'
				#print file_name
				buffer.require_buffer(file_name, \
					int(re_dict["time_sec"]),\
					int(re_dict["time_usec"]),\
					float(re_dict["duration"]),\
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
		get_max_size(line, cache_obj)
	fd.seek(0, 0)

	for line in fd:
		time+=get_value(line, cache_obj)
	fd.close()
	cache_obj.print_final_result()
if __name__ == '__main__':
	main(sys.argv[1], sys.argv[2])
