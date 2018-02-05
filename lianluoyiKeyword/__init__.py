import time
import redis

class Node(object):
    def __init__(self):
        self.children = None

# The encode of word is UTF-8
def add_word(root,word):
    node = root
    for i in range(len(word)):
        if node.children == None:
            node.children = {}
            node.children[word[i]] = Node()

        elif word[i] not in node.children:
            node.children[word[i]] = Node()

        node = node.children[word[i]]

def init():
    root = Node()
    pool = redis.ConnectionPool(host='192.168.1.202', port=6679,password='lianluoyi',db=0)
    r = redis.Redis(connection_pool=pool)
    fp = r.get("sensitiveWords").decode()
    result = fp.split(',')
    for line in result:
        # line = line[0:-1]
        #print len(line)
        #print line
        #print type(line)
        add_word(root,line)
    # fp.close()
    return root

# The encode of word is UTF-8
# The encode of message is UTF-8
def is_contain(message, root):
    dict = {}
    keyWord={}
    for i in range(len(message)):
        p = root
        j = i
        while (j<len(message) and p.children!=None and message[j] in p.children):
            p = p.children[message[j]]
            j = j + 1
        if p.children==None:
            print ('---word---',message[i:j])
            keyWord[j]=message[i:j]
    if keyWord :
        dict[True]=keyWord
    else:
        dict[False]=False
    return dict



def dfa(msg):
    print ('----------------dfa-----------')
    root = init()
    message = msg
    #message = '不顾'
    # print ('***message***',len(message))
    start_time = time.time()
    # for i in range(1000):
    res = is_contain(message,root)
    # print (res)
    end_time = time.time()
    # print (end_time - start_time)
    return res

def is_contain2(message,word_list):
    for item in word_list:
        if message.find(item)!=-1:
            print('---word---',item)
            return True
    return False

def normal():
    print ('------------normal--------------')
    path = 'D:/敏感词库大全.txt'
    fp = open(path,'r')
    word_list = []
    message = '四处乱咬乱吠，吓得家中11岁的女儿躲在屋里不敢出来，直到辖区派出所民警赶到后，才将孩子从屋中救出。最后在征得主人同意后，民警和村民合力将这只发疯的狗打死'
    print ('***message***',len(message))
    for line in fp:
        line = line[0:-1]
        word_list.append(line)
    fp.close()
    print('The count of word:',len(word_list))
    start_time = time.time()
    for i in range(1000):
        res = is_contain2(message,word_list)
    #print res
    end_time = time.time()
    print (end_time - start_time)


if __name__ == '__main__':
    dfa("四处乱咬乱吠，吓得家中11岁的女儿躲在屋里不敢出来，直到辖区派出所民警赶到后，才将孩子从屋中救出。最后在征得主人同意后，民警和村民合力将这只发疯的狗打死'")
    # normal()
