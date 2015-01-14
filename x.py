"Read SequenceFile encoded as TypedBytes, store files by key (assumes they are unique) with data as the value"
import hadoopy
import os
 
local_path = '.'
 
 
def main():
    hdfs_path = 'hdfs:///recommendation/actions/reads/last_24/rt-actions-read-2015_01_12_20.log'
    for key, value in hadoopy.readtb(hdfs_path):
        path = os.path.join(local_path, key)
	print path
        try:
            os.makedirs(os.path.dirname(path))
        except OSError:
            pass
        open(path, 'w').write(value)
 
if __name__ == '__main__':
    main()
