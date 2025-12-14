import os,time
def watch(filepath="test.txt"):
    mtime=os.path.getmtime(filepath)
    while True:
        if os.path.getmtime(filepath)!=mtime:
            mtime=os.path.getmtime(filepath)
            print(open(filepath).read().strip())
        time.sleep(0.5)
if __name__=='__main__':watch()