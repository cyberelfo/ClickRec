import timeit
import csv
import time
from pprint import pprint
from bitarray import bitarray

path = '/Users/franklin/Downloads/'
filename = 'rt-actions-read-2014_11_21_16.log'
# filename = 'teste.log'

dictionary = {} # armazenar todos os documentos 
users = []
window_size = 1000

def populate_array(size, user_id, document_id):

	update_documents(size, document_id)


def fast_update_documents(size, document_id, dictionary):
	dictionary = dict(map((lambda (k,x): (k, x + bitarray(1))), dictionary.iteritems()))

	try:
		dictionary[document_id].extend([True])
	except KeyError:
 		dictionary[document_id] = bitarray([False] * (size - 1))
 		dictionary[document_id].extend([True])

 	return dictionary

def test_update_documents(size, document_id):
	size +=1 
	updated = False
	for key in dictionary:
		if key == document_id:
			dictionary[key] = 1
			updated = True
		else:
			dictionary[key] += 1

	if not updated:
 		dictionary[document_id] = size

def load_window(size, document_id, user_id):
	size +=1 
	updated = False
	for key in dictionary:
		if key == document_id:
			dictionary[key].extend([True])
			updated = True
		else:
			dictionary[key].extend([False])

	if not updated:
 		dictionary[document_id] = bitarray([False] * (size - 1))
 		dictionary[document_id].extend([True])

 	if user_id not in users:
 		users.append(user_id)

def slide_window(size, document_id, user_id):
 	if user_id in users:
 		a = users.index(user_id)
 		if document_id in dictionary:
	 		dictionary[document_id][a] = 1
	 		# print "Update!"
	 	else:
	 		dictionary[document_id] = bitarray([False] * (size))
	 		dictionary[document_id][a] = 1
	 		# print "New!"
	else:
		updated = False
	 	# print "else"
	 	del users[0]
	 	users.append(user_id)

		for key in dictionary.keys():
			del dictionary[key][0]
			if key == document_id:
				dictionary[key].extend([True])
				updated = True
			else:
				if dictionary[key].count() == 0:
					del dictionary[key]
				else:
					dictionary[key].extend([False])

		if not updated:
	 		dictionary[document_id] = bitarray([False] * (size - 1))
	 		dictionary[document_id].extend([True])

if __name__ == '__main__':

	start = timeit.default_timer()
	start_t = timeit.default_timer()

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	for row in enumerate(reader):
		# print row
		if row[0] > 10000: 
			break
		if len(users) < window_size:
			load_window(row[0], row[1][4], row[1][2])
		else:
			slide_window(row[0], row[1][4], row[1][2])
		if row[0] % 1000 == 0:
			print row[0]
			stop_t = timeit.default_timer()
			tempo_execucao = stop_t - start_t
			print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
			start_t = stop_t



	f.close()

	print "Users:", len(users)
	print "Docs:", len(dictionary)

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	# pprint(dictionary)
	# print users

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
