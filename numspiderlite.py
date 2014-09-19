#!/usr/bin/python
#-*- coding:utf-8 -*-
"""手机号段爬虫：接收用户命令参数精简版 for sqlitedb
@version:1.0
@author:Kenny{Kenny.F<mailto:kennyffly@gmail.com>}
@since:2014/05/23
"""
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import gevent 						#gevent协程包
import multiprocessing				#多进程
from multiprocessing import Manager
import urllib2
from urllib import unquote,quote
import socket
socket.setdefaulttimeout(20)
import cookielib
import random
import simplejson as json
import os
import time
import sqlite3						#sqlite数据库操作
from functools import wraps			#方法工具
from strtodecode import strtodecode	#编码检测转换


manager = Manager()					#多进程共享队列
lacknumlist = manager.list()


def multi_run_wrapper(func):		#多进程map包裹参数
	@wraps(func)
	def newF(args):
		if isinstance(args,list):
			return func(*args)
		elif isinstance(args,tuple):
			return func(*args)
		else:
			return func(args)
	return newF


def getRanIp():		#得到随机IP
	#123.125.40.255 - 123.127.134.56 北京联通154938条
	return "123.{0}.{1}.{2}".format(random.randint(125,127), random.randint(40,134), random.randint(56,255))


def _cookiePool(url):		#查看cookie池
	cookie = cookielib.CookieJar()
	opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))
	opener.open(url)
	for item in cookie:
		print 'Name = '+item.name
		print 'Value = '+item.value


def catchPage(url=''):		#封装的网页页面获取
	if not url:
		return False

	with open("./logs/outprint.txt","a") as f:
		f.write(url+"\n")

	try:
		headers = {
			'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
			'Referer':'http://www.baidu.com',
			"X-Forwarded-For":getRanIp()
		}
		req = urllib2.Request(
			url = url,
			headers = headers
		)

		html = ''
		result = ''
		try:
			try:
				gevent.Timeout
			except:
				result = urllib2.urlopen(req,timeout=20)
			else:
				with gevent.Timeout(20, False):
					result = urllib2.urlopen(req)
		except urllib2.HTTPError, e:
			#For Ptyhon 2.6
			try:
				socket.timeout
			except:
				print 'The server couldn\'t fulfill the request.'
				print "url:{0} Httperrorcode:{1}".format(url, e.code)
			else:
				if isinstance(e.reason, socket.timeout):
					print 'The server couldn\'t fulfill the request.'
					print "url:{0} Httperrorcode:{1}".format(url, e.code)
		except urllib2.URLError, e:
		    print 'We failed to reach a server.'
		    print "url:{0} Reason:{1}".format(url, e.reason)
		except socket.timeout, e:
			#For Python 2.7
			print 'The server couldn\'t fulfill the request.'
			print "url:{0} Httperrorcode:{1}".format(url, e)
		else:
			if result:
				html = result.read()
		return html
	except:
		try:
			socket.timeout
		except:
			print 'The server couldn\'t fulfill the request.'
			print "url:{0} Httperrorcode:{1}".format(url, 'timeout')
		else:
			print 'The server couldn\'t fulfill the request.'
			print "url:{0} Server someting error".format(url)
		return False


def opensqlitedb():		#从sqlite数据源开始工作
	db_file = './data/mobile_area.db'

	if not os.path.exists(db_file):
		try:
			cx = sqlite3.connect(db_file)
			cu = cx.cursor()
			#建表
			sql = "create table mobile_area (id integer primary key,\
										mobile_num integer,\
										mobile_area varchar(50) NULL,\
										mobile_type varchar(50) NULL,\
										area_code varchar(50) NULL,\
										post_code varchar(50) NULL)"
			cu.execute(sql)
		except:
			print "can not find sqlite db file\n"
			with open('./logs/errorlog.txt','a') as f:
				f.write("can not find sqlite db file '%s'\n" % str(db_file))
			return False
	else:
		try:
			cx = sqlite3.connect(db_file)
			cx.text_factory = str
			cu = cx.cursor()
		except:
			print "can not find sqlite db file\n"
			with open('./logs/errorlog.txt','a') as f:
				f.write("can not find sqlite db file '%s'\n" % str(db_file))
			return False

	mobile_err_list,mobile_dict = [],{}
	limit = 10000
	offset = 0
	mobile_num_pre = 0
	while 1:
		cu.execute("SELECT * FROM mobile_area ORDER BY mobile_num ASC LIMIT %d OFFSET %d " % (limit, offset))
		rs = cu.fetchall()
		if not rs:
			break
		else:
			offset = offset + limit
			for i in xrange(0,len(rs)):
				id = rs[i][0]
				mobile_num = int(rs[i][1])
				mobile_area = rs[i][2]
				mobile_type = rs[i][3]
				area_code = rs[i][4]
				post_code = rs[i][5]

				if len(mobile_area) > 100 or (not mobile_area)  or (not mobile_num) or len(mobile_type) > 100 or len(area_code) > 100 or len(post_code) > 100 or len(str(mobile_num)) > 7:
					print "error id:%d" % id
					continue

				#正确的号码入字典
				mobile_dict[str(mobile_num)] = True

	print "get data from sqlite works down!\n"
	return mobile_dict


@multi_run_wrapper
def getNumPage(segnum='', num='', url=''):		#获取号码页详细数据
	if not segnum:
		return False
	if not num:
		return False
	if not url:
		return False

	gevent.sleep(random.randint(10,22)*0.81)	#从此处协程并发

	db_file = './data/mobile_area.db'

	html = catchPage(url)
	if not html:
		print "catch %s num page error!" % num
		print "url:%s\n" % (url)
		with open("./logs/errornum.txt", "a") as f:
			f.write(segnum+','+num+','+url+"\n")
		return False

	#json数据
	try:
		page_temp_dict = json.loads(unquote(html))
	except:
		print segnum+','+num+','+url+",result error convert to dict\n"
		with open('./logs/errorlog.txt','a') as f:
			f.write(segnum+','+num+','+url+",result error convert to dict\n")
		return False
	else:
		try:
			cx = sqlite3.connect(db_file)
			cu = cx.cursor()
		except:
			print "can not find sqlite db file\n"
			with open('./logs/errorlog.txt','a') as f:
				f.write("can not find sqlite db file '%s'\n" % str(db_file))
			return False

		insdata = {}
		#mobile_num
		if page_temp_dict.get('Mobile', False):
			insdata['mobile_num'] = int(page_temp_dict['Mobile'])
		else:
			with open('./logs/errorlog.txt','a') as f:
				f.write(segnum+','+num+','+url+",No matching data\n")
			return False	#无号码
		#mobile_area
		if page_temp_dict.get('Province', False):
			if page_temp_dict['Province'] == u'未知':
				with open('./logs/errorlog.txt','a') as f:
					f.write(segnum+','+num+','+url+",province is weizhi\n")
				return False	#无地区
			if page_temp_dict.get('City', False):
				insdata['mobile_area'] = strtodecode(page_temp_dict['Province']+' '+page_temp_dict['City'])
			else:
				insdata['mobile_area'] = strtodecode(page_temp_dict['Province']+' '+page_temp_dict['Province'])
		else:
			with open('./logs/errorlog.txt','a') as f:
				f.write(segnum+','+num+','+url+",No matching province\n")
			return False	#无地区
		#mobile_type
		if page_temp_dict.get('Corp', False):
			if page_temp_dict.get('Card', False):
				insdata['mobile_type'] = strtodecode(page_temp_dict['Corp']+' '+page_temp_dict['Card'])
			else:
				insdata['mobile_type'] = strtodecode(page_temp_dict['Corp'])
		#area_code
		if page_temp_dict.get('AreaCode', False):
			insdata['area_code'] = strtodecode(page_temp_dict['AreaCode'])
		#post_code
		if page_temp_dict.get('PostCode', False):
			insdata['post_code'] = strtodecode(page_temp_dict['PostCode'])

		if insdata:
			sql = "insert into mobile_area values (?,?,?,?,?,?)"
			cu.execute(sql, (None,insdata['mobile_num'],insdata['mobile_area'],insdata['mobile_type'],insdata['area_code'],insdata['post_code']))

			try:
				cx.commit()		#执行insert
			except:
				with open('./logs/errorlog.txt','a') as f:
					f.write(segnum+','+num+','+url+",insert sqlitdb faild\n")
				return False
			else:
				print "%d write db ok" % insdata['mobile_num']
				return True

def getneednum(url='', step=10):		#获取所有未记录的号码信息数据
	if not lacknumlist:
		return False
	if not url:
		return False
	if not step:
		print "step can not be null"
		return False
	if not isinstance(step,int):
		print "step should be numeric"
		return False
	if step < 0:
		print "step should be > 0"
		return False

	offset = 0
	limit = int(step)
	len_max = len(lacknumlist)
	breaktag = False
	while 1:
		if breaktag:
			break

		threads = []
		for i in xrange(offset,(limit+offset)):
			try:
				num = lacknumlist[i]
			except:
				breaktag = True
				break
			else:
				furl = url()
				threads.append( gevent.spawn(getNumPage, (num[0:3], num, furl+num)) )		#协程并发

		try:
			gevent.joinall(threads)
			# print "%d-%d is end\n" % (offset+1,limit+offset)
		except Exception as e:
			print "Gevent catch error\n"

		offset = offset + limit
		time.sleep(random.randint(5,80)*0.9)

	i = 1 									#处理网络异常号码数据10次
	while i <= 10:
		if not os.path.exists("./logs/errornum.txt"):
			break
		j = 1
		threads = []
		with open("./logs/errornum.txt","r") as f:
			while 1:
				if (j >= step) and threads:
					try:
						gevent.joinall(threads)
					except Exception as e:
						print "turn%d-%d Gevent catch error\n" % (i,j)
					time.sleep(random.randint(5,80)*0.9)
					threads = []
					j = 0
				line = f.readline()
				if line:
					errnum_str = line.strip()
					errnum_truple = errnum_str.split(',')
					threads.append(gevent.spawn(getNumPage, (errnum_truple[0], errnum_truple[1], errnum_truple[2])))
				else:
					if threads:
						try:
							gevent.joinall(threads)
						except Exception as e:
							print "turn%d-%d Gevent catch error\n" % (i,j)
					break
				j += 1

		if i < 10:
			with open("./logs/errornum.txt","w") as f:		#清除文件内容
				pass
		i = i + 1


def setneednum(num='', mobile_dict={}):		#设置得到所有未补全的号码
	if not num:
		return False

	if len(str(num))==3:
		start_num = int(num+'0000')
		end_num = int(num+'9999')
	else:
		num_list = num.split('-')
		start_num = int(num_list[0])
		end_num = int(num_list[1])

	i = start_num
	while i <= end_num:
		if not mobile_dict.get(str(i),False):		#查找没有的号码
			lacknumlist.append(str(i))
		i += 1
	# print "%s num works down\n" % num


def setsegnum(segnumlist=[], mobile_dict={}):		#根据号段起并发进程
	if not segnumlist:
		return False

	record = []
	for seg in xrange(0, len(segnumlist)):
		segnum = segnumlist[seg].strip()
		if len(str(segnum)) == 3:		#指定的单个号段:137
			try:
				int(segnum)
			except:
				print "%s is illegal argument\n" % str(segnum)
				continue
			else:
				process = multiprocessing.Process(target=setneednum, args=(str(segnum), mobile_dict))
				process.start()
				record.append(process)
		elif len(str(segnum)) == 7:		#具体指定的单个号码:1391234
			if not mobile_dict.get(str(segnum),False):
				lacknumlist.append(str(segnum)) #sqlite没有的号码
		else:
			segparam_list = segnum.split('-')
			try:
				int(segparam_list[0])
			except:
				print "%s is illegal argument\n" % str(segnum)
				continue
			else:
				try:
					segparam_list[1]
				except:
					print "%s is illegal argument\n" % str(segnum)
					continue
				else:
					if segparam_list[0][:3] == segparam_list[1][:3] :		#指定号码范围:1380000-1389999
						process = multiprocessing.Process(target=setneednum, args=(str(segnum), mobile_dict))
						process.start()
						record.append(process)
					else:
						print "%s is illegal argument\n" % str(segnum)
						continue
	for process in record:
		process.join()

	print "all SegNum prepare works down!\n"


def callback_url_showji():		#返回showji网的api地址
	showji = 'http://api.showji.com/Locating/www.showji.c.o.m.aspx?output=json'
	return "{0}&timestamp={1}&m=".format(showji, int(time.time()))


def main(param=''):		#主方法
	with open("./logs/errornum.txt","w") as f:		#清除零时文件内容
		pass
	with open("./logs/outprint.txt","w") as f:
		pass

	if not param:
		print "no argument！"
		return False

	# segnumlist = [\
	# 			# '134','135','136','137','138','139','147','150','151','152','157','158','159','182','183','187','188',\
	# 			# '130','131','132','136','145','185','186',\
	# 			# '133','153','180','189',\
	# 			# '147','155','156','170','176','177','178','181','184'\
	# 			]

	segnumlist = str(param).split(',')

	#从sqlite库查已有的
	mobile_dict = opensqlitedb()

	#算哪些是还没有的
	setsegnum(segnumlist, mobile_dict)
	if lacknumlist:
		tempstr = ''
		for i in xrange(0,len(lacknumlist)):
			tempstr += str(lacknumlist[i])+"\n"
		with open("./logs/needmobilelist.txt","w") as f:
			f.write(tempstr)

	#补没有的
	getneednum(callback_url_showji)

	print "all works end!"


if __name__ == "__main__":
	from optparse import OptionParser
	USAGE = "usage:python numspiderlist.py -s [String, e.g:138,137,1393134,1700001-1709999,1450000-1459999]"
	parser = OptionParser(USAGE)
	parser.add_option("-s", dest="s")
	opt,args = parser.parse_args()
	judopt = lambda x:x.s

	if not opt.s:
		print USAGE
		sys.exit(1)

	if not judopt(opt):
		print USAGE
		sys.exit(1)

	if opt.s:
		content = opt.s

	main(content)
