class Instance:

    def __init__(self,ip,port,name,master):
        self.ip = ip
        self.port = port
        self.name = name
        self.master = master


i1 = Instance('127.0.0.1',3306,'cluster1','')
i2 = Instance('127.0.0.2',3306,'cluster1','127.0.0.1')

i3 = Instance('127.0.0.3',3306,'cluster1','127.0.0.2')
i4 = Instance('127.0.0.4',3306,'cluster1','127.0.0.2')

i5 = Instance('127.0.0.5',3306,'cluster1','127.0.0.3')
i6 = Instance('127.0.0.6',3306,'cluster1','127.0.0.3')

i7 = Instance('127.0.0.7',3306,'cluster1','127.0.0.4')
i8 = Instance('127.0.0.8',3306,'cluster1','127.0.0.4')

i9 = Instance('127.0.0.9',3306,'cluster1','127.0.0.1')

i10 = Instance('127.0.0.10',3306,'cluster1','127.0.0.8')

list = [i1,i2,i3,i4,i5,i6,i7,i8,i9,i10]

def printcluster(list):

    for i in list:
        if i.master=="":
            master = i
            print("master:",i.ip)
    findnext(master,list,1)

def findnext(instance,list,level):

    this_level=[]
    for i in list:
        # print("i.master",i.master,"master ip",instance.ip)
        if i.master == instance.ip:
            print(level*'\t',"slave:",i.ip)
            findnext(i, list, level+1)
            this_level.append(i)

printcluster(list)