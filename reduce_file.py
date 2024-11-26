import queue
import threading

import pyspark

#when the dataset is in 1gb then reduce the dataset size using below steps,

# remove the numbers, repeatlines, sort the datasets, split the words, remove the unnecessary words

def multi_thread_function(func,map1_input,map2_input):
  my_queue1 = queue.Queue()
  my_queue2 = queue.Queue()
  t1 = threading.Thread(target=func, args=(map1_input,my_queue1))
  t2 = threading.Thread(target=func, args=(map2_input,my_queue2))
  t1.start()
  t2.start()
  t1.join()
  t2.join()
  list1out = my_queue1.get()
  list2out = my_queue2.get()
  return list1out,list2out

def splitlines(text,a):
  linessplit = text.splitlines() #Splitting the lines into a list
  split1 = linessplit[0:a]       #Creating the first split with the first "a" number of lines into split 1
  split2 = linessplit[a:]        #Creating the second split with the first "a" number of lines into split 2
  return split1,split2
def data_clean(text):
  NoNumbers = ''.join([i for i in text if not i.isdigit()]) #Removing numbers
  NoNumbers = text.lower()                                  #Making the text to lower case
  import re
  onlyText = re.sub(r"[^a-z\s]+",' ',NoNumbers)             #Removing punctuation
  finaltext = "".join([s for s in onlyText.strip().splitlines(True) if s.strip()]) #Removing the null lines
  return finaltext

def mapper(text,out_queue):
  keyval = []
  for i in text:
    wordssplit = i.split()
    for j in wordssplit:
      keyval.append([j,1])      #Appending each word in the line with 1 and storing it in ["word",1] format in a nested list
  out_queue.put(keyval)


def reducer(part_out1,out_queue) :
  sum_reduced = []
  count = 1
  for i in range(0,len(part_out1)):
    if i < len(part_out1)-1:
      if part_out1[i] == part_out1[i+1]:
       count = count+1                              #Counting the number of words
      else :
       sum_reduced.append([part_out1[i][0],count])  #Appending the word along with count to sum_reduced list as ["word",count]
       count = 1
    else: sum_reduced.append(part_out1[i])          #Appending the last word to the output list
  out_queue.put(sum_reduced)

def sortedlists(list1,list2):
  out = list1 + list2             #Appending the two input lists into a single list
  out.sort(key= lambda x :x[0])   #Sorting the lists based on the first element of the list which is the "word"
  return out

def partition(sorted_list) :
 sort1out = []
 sort2out = []
 for i in sorted_list:
    if i[0][0] < 'n':             #Partitioning the sorted word list into two separate lists
      sort1out.append(i)          #with first list containing words starting with a-m and n-z into second
    else : sort2out.append(i)
 return sort1out,sort2out

def ReducearraySize(items):
    cleantext = data_clean(items)
    linessplit = splitlines(cleantext, 5000)
    mapperout = multi_thread_function(mapper, linessplit[0], linessplit[1])
    sortedwords = sortedlists(mapperout[0], mapperout[1])
    slicedwords = partition(sortedwords)
    reducerout = multi_thread_function(reducer, slicedwords[0], slicedwords[1])
    return reducerout[0] + reducerout[1]







