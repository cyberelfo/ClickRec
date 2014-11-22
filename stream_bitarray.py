import timeit
import csv
import time
from pprint import pprint
from bitarray import bitarray

path = '/Users/franklin/Downloads/'
filename = 'rt-actions-read-2014_11_21_16.log'

dictionary = {} # armazenar todos os documentos 

# def update_documents(size, document_id):
# 	size += 1
# 	# print "Creating/updating ", document_id
# 	if dictionary.has_key(document_id):
# 		dictionary[document_id].extend([True])
# 	else:
# 		dictionary[document_id] = bitarray([False] * (size - 1))
# 		dictionary[document_id].extend([True])

# 	for key in dictionary.keys():
# 		if key != document_id:
# 			dictionary[key].extend([False])

def fast_update_documents(size, document_id):
	size +=1 
	updated = False
	for key in dictionary.keys():
		if key == document_id:
			dictionary[key].extend([True])
			updated = True
		else:
			dictionary[key].extend([False])
			updated = True

	if not updated:
 		dictionary[document_id] = bitarray([False] * (size - 1))
 		dictionary[document_id].extend([True])

if __name__ == '__main__':

	start = timeit.default_timer()

	f = open(path+filename, 'rb')

	reader = csv.reader(f)

	for row in enumerate(reader):
		# if row[0] >= 5: 
		# 	break
		fast_update_documents(row[0], row[1][4])
		if row[0] % 1000 == 0:
			print row[0]

	f.close()

	stop = timeit.default_timer()
	tempo_execucao = stop - start 

	# pprint(dictionary)

	print "Fim processamento"
	print "Tempo de execucao:", time.strftime('%Hhs %Mmin %Sseg', time.gmtime(tempo_execucao))
