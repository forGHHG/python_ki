
# coding: utf-8

# usage> csv2mongo --db DB --collection COLLECTION -u USER -p PASSWORD --host HOST:port --index="0:,1,2,3" FILENAME1.csv

import csv
import pymongo
import argparse
import time
import traceback
import re


def main():
    args = parse_user_argument()
    args.verbose = True
    (pk_list, index_list) = parse_index_string(args.index)
    
    try:
        connection = pymongo.MongoClient('mongodb://' + args.host)
        db = connection[args.db]
        db.authenticate(args.user, args.password)
        if args.verbose:
            print('Connected to', args.host,  'as a user', args.user)
        collection = db[args.collection]
    except pymongo.errors.OperationFailure as e:
        print(e)
    except Exception as e:
        print('ERROR:', e)
    else:
        write_mongo(args.file[0], index_list, pk_list, collection)
    finally:
        connection.close()
        if args.verbose:
            print('Disconnected from', args.host)

def parse_user_argument():
    parser = argparse.ArgumentParser()
    parser.add_argument('-u', '--user', required=True)
    parser.add_argument('-p', '--password', required=True)
    parser.add_argument('--host', required=True)
    parser.add_argument('--db', required=True)
    parser.add_argument('--collection', required=True)
    parser.add_argument('--index', required=False)
    parser.add_argument('file', nargs=1)
    
    args = parser.parse_args()

    return args


def parse_index_string(indexStr):
    pk_list = []
    index_list = []
    
    if indexStr:
        index = re.findall('\d+:?', indexStr)
        
        for i in index:
            i_value = i
            if ':' is i_value[-1]:
                i_value = int(i_value[:-1])
                pk_list.append(i_value)        
                index_list.append(i_value)
            else:
                index_list.append(int(i))
                
    return pk_list, index_list

    

def write_mongo(file_name, index_list, pk_list, collection):    
    try:    
        with open(file_name, 'r', encoding= 'utf-8') as f:
            rdr = csv.reader(f)
            print('file_name:', file_name)
            nLines = sum(1 for e in rdr)
            f.seek(0)
            
            count = 0
            
            if nLines > 1:
                field = next(rdr) # get field name
     
                if index_list is None:
                    index_list = range(0, len(field))                
        
                if pk_list:
                    pk_to_Index = []
                    for pk in pk_list:
                        pk_to_Index.append((field[pk], pymongo.ASCENDING))
                    print('PK:', pk_to_Index)
                    collection.create_index(pk_to_Index, unique=True)
                
                for idx, line in enumerate(rdr):
                    myDic = {}
                    for i in index_list:
                        myDic[field[i]] = line[i]
                
                    try:
                        collection.insert_one(myDic)
                    except pymongo.errors.DuplicateKeyError:
                       print('duplicate: Line {_lineNum} is not inserted. (_id: {_id})'.format(_lineNum = (idx+1), _id=myDic['_id']))
                    else:
                        count = count + 1
                        continue
                    
#                    if idx > 3:
#                        break;
            
                print(count, 'document(s) is inserted')
                
    except FileNotFoundError:
        print('FileNotFoundError:', file_name)
    except Exception as e:
        print('ERROR:', e)
        traceback.print_exc()   

if __name__ == '__main__':
    start_time = time.time()
    print('START')
    
    main()
    
    print('OK')
    print('%1.2f sec' %(time.time() - start_time))