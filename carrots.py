'''
Copyright 2020 Romeo Dabok

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
'''
import os
import zipfile
import tarfile
import gc
import io
from os.path import join, getsize
import time
#Oh the version of this carrot
VERSION = "0.6"
class carrotSearch:
    '''Creates a carrotSearch object that can be used to search the searchPath for the searchString.'''
    def __init__(self,searchPath,searchString,zips=True,tars=True,CASE_SENSITIVE=False,COLLECT_DIRS=True,COLLECT_FILES=True,COLLECT_FILENAME=True,
                 COLLECT_FILES_CONTENT=True,ENTER_ARCHIVES=True,COUNT_HITS=True,genExcludeExtensions=[],genOnlyExtensions=[],
                 fileExcludeExtensions=[],fileOnlyExtensions=[],arcFileExcludeExtensions=[],arcFileOnlyExtensions=[]):
        #To raise an exception if one of the flags provided is not a boolean.
        #I know, this is not necessary but I was just teaching myself how to raise
        #exceptions
        for k,v in {"CASE_SENSITIVE":CASE_SENSITIVE,"COLLECT_DIRS":COLLECT_DIRS,"COLLECT_FILES":COLLECT_FILES,
                    "COLLECT_FILENAME":COLLECT_FILENAME,"COLLECT_FILES_CONTENT":COLLECT_FILES_CONTENT,
                    "ENTER_ARCHIVES":ENTER_ARCHIVES,"COUNT_HITS":COUNT_HITS}.items():
            if not isinstance(v,bool):
                raise ArgumentError('%s expected for %s, got %s %s instead' % (bool,k,type(v),v))
        if not COLLECT_DIRS and not COLLECT_FILES:
            raise ArgumentError('At least one of COLLECT_DIRS or COLLECT_FILES must be True')
        if len(genExcludeExtensions) > 0 and len(genOnlyExtensions) > 0:
            raise ArgumentError('You cannot use both genExcludeExtensions and genOnlyExtensions at the same time.')
        self.searchPath = searchPath
        self.searchString = searchString
        self.__searchBytes = bytes(self.searchString,'utf-8')
        self.__CASE_SENSITIVE=CASE_SENSITIVE
        self.__COLLECT_DIRS=COLLECT_DIRS
        self.__COLLECT_FILES_FILENAME=COLLECT_FILENAME
        self.__COLLECT_FILES=COLLECT_FILES
        self.__COLLECT_FILES_CONTENT=COLLECT_FILES_CONTENT
        self.__ENTER_ARCHIVES=ENTER_ARCHIVES
        self.__COUNT_HITS=COUNT_HITS
        self.errors=[]
        self.__archs = []
        self.__hitFiles = 0
        self.__zips = zips
        self.__tars = tars
        self.__afileXdeExt = arcFileExcludeExtensions
        self.__afileIncExt = arcFileOnlyExtensions
        self.__fileXdeExt = fileExcludeExtensions
        self.__fileIncExt = fileOnlyExtensions
        self.__genXdeExt = genExcludeExtensions
        self.__genIncExt = genOnlyExtensions
        self.hits = {
            "files":[],
            "arcFiles":[],
            "directories":[],
        }
        self.startTime = None
        self.__filetr = 0
        self.__SEARCH_STATUS = 0
        self.__offCheck = 0
        
    def __checkExt__(self,path,llist):
        if len(llist) < 1:
            return False
        else:
            fn = path.split('.')
            for ex in llist:
                if fn[len(fn)-1].strip() == ex.strip():
                    return True
                
    def __checkIncExclExt__(self,path,inlist,exlist):
        if len(exlist) > 0:
            return not self.__checkExt__(path,exlist)
        elif len(inlist) > 0:
            return self.__checkExt__(path,inlist)
        else:
            return True
    
    def getTotalHits(self):
        return self.__hitFiles
    
    def getScannedNum(self):
        return self.__filetr
    
    def start(self):
        self.__SEARCH_STATUS = 1
        gc.collect()
        self.__carrotdig__()
        
    def stop(self):
        self.__SEARCH_STATUS = 2
        
    def endAction(self):
        print("DONE")
        print("Scanned ",self.getScannedNum(),"files")
        print("in",self.endTime-self.startTime,"seconds")
        print("got",self.getTotalHits(),"hits")
        
    def onFile(self,path):
        pass
    
    def getStatus(self):
        return self.__SEARCH_STATUS
    
    def __compare__(self,a,b):
        s1, s2 = a,b
        if not self.__CASE_SENSITIVE:
            try:
                s1, s2 = s1.lower(), s2.lower()
            except:
                pass
        if self.__COUNT_HITS:
            return s2.count(s1)
        else:
            if s1 in s2:
                return 1
            else:
                return 0
            
    def __ziperate__(self,root,at=''):
        try:
            pathObject = root.oref
            ainfo = pathObject.infolist()
            if self.__COLLECT_FILES:
                for file in ainfo:
                    fullPath=join(pathObject.filename,file.filename)
                    #########CHECK#IF#DONE##########
                    if self.__SEARCH_STATUS == 2:
                        self.__checkDone__()
                        break
                    if not self.__checkIncExclExt__(fullPath,self.__genIncExt,self.__genXdeExt):
                        continue
                    if not self.__checkIncExclExt__(fullPath,self.__afileIncExt,self.__afileXdeExt):
                        continue
                    ################################
                    self.onFile(fullPath)
                    self.__filetr += 1
                    tmp = archivedFile(join(pathObject.filename,file.filename),file.filename,pathObject.filename)
                    if self.__COLLECT_FILES_FILENAME:
                        tmp.titleHits+=self.__compare__(self.searchString,file.filename)
                    if self.__COLLECT_FILES_CONTENT and (self.__COUNT_HITS or (not self.__COUNT_HITS and tmp.totalHits() < 1)):
                        #tmp.contentHits+=self.__compare__(self.__searchBytes,pathObject.read(file))
                        with pathObject.open(file.filename,mode='r',force_zip64=True) as f:
                            tmp.contentHits+=self.__compare__(self.__searchBytes,f.read())
                    if tmp.totalHits() > 0:
                        self.hits["arcFiles"].append(tmp)
                        self.__hitFiles += 1
                    else:
                        self.__nullise__(tmp)
        except ValueError as e:
            self.errorEncountered("ValueError",e,root.oref.filename)
        return True
    
    def __getSize__(self,bytes, suffix="B"):
        factor = 1024
        for unit in ["", "K", "M", "G", "T", "P"]:
            if bytes < factor:
                return f"{bytes:.2f}{unit}{suffix}"
            bytes /= factor
            
    def __tarerate__(self,root,at=''):
        try:
            pathObject = root.oref
            ainfo = pathObject.getmembers()
            if self.__COLLECT_FILES:
                for file in ainfo:
                    fullPath=join(pathObject.name,file.name)
                    #########CHECK#IF#DONE##########
                    if self.__SEARCH_STATUS == 2:
                        self.__checkDone__()
                        break
                    if not self.__checkIncExclExt__(fullPath,self.__genIncExt,self.__genXdeExt):
                        continue
                    if not self.__checkIncExclExt__(fullPath,self.__afileIncExt,self.__afileXdeExt):
                        continue
                    ################################
                    self.onFile(fullPath)
                    self.__filetr += 1
                    tmp = archivedFile(join(pathObject.name,file.name),file.name,pathObject.name)
                    if self.__COLLECT_FILES_FILENAME:
                        tmp.titleHits+=self.__compare__(self.searchString,file.name)
                    if tmp.totalHits() > 0:
                        self.hits["arcFiles"].append(tmp)
                        self.__hitFiles += 1
                    else:
                        self.__nullise__(tmp)
        except ValueError as e:
            self.errorEncountered("ValueError",e,root.oref.filename)
        except MemoryError as e:
            self.errorEncountered("MemoryError",e,root.oref.filename)

    def switchingPhase(self):
        gc.collect()
        self.__readExtra__()
        self.__waitForArchs__()
        
    def __carrotdig__(self):
        self.startTime=time.time()
        self.__extra = []
        for root, dirs, files in os.walk(self.searchPath):
            #########CHECK#IF#DONE##########
            if self.__SEARCH_STATUS == 2:
                self.__checkDone__()
                break
            ################################
            if self.__COLLECT_DIRS:
                self.__filetr += 1
                for fo in dirs:
                    #########CHECK#IF#DONE##########
                    fullPath = join(root,fo)
                    self.onFile(fullPath)
                    if self.__SEARCH_STATUS == 2:
                        self.__checkDone__()
                        break
                    if not self.__checkIncExclExt__(fullPath,self.__genIncExt,self.__genXdeExt):
                        continue
                    ################################
                    tmp = normalFile(fullPath,fo)
                    tmp.titleHits = self.__compare__(self.searchString,fo)
                    if tmp.titleHits > 0:
                        self.hits["directories"].append(tmp)
                        self.__hitFiles+=1
                    else:
                        self.__nullise__(tmp)
            if self.__COLLECT_FILES:
                self.__filetr += 1
                for file in files:
                    fullPath = join(root,file)
                    self.onFile(fullPath)
                    #########CHECK#IF#DONE##########
                    if self.__SEARCH_STATUS == 2:
                        self.__checkDone__()
                        break
                    if not self.__checkIncExclExt__(fullPath,self.__genIncExt,self.__genXdeExt):
                        continue
                    if not self.__checkIncExclExt__(fullPath,self.__fileIncExt,self.__fileXdeExt):
                        continue
                    ################################
                    tmp = normalFile(fullPath,file)
                    if self.__COLLECT_FILES_FILENAME:
                        tmp.titleHits+=self.__compare__(self.searchString,file)
                    if self.__ENTER_ARCHIVES and self.__zips and zipfile.is_zipfile(fullPath):
                        self.__archs.append(scanArchFile(fullPath,"zip"))
                    elif self.__ENTER_ARCHIVES and self.__tars and tarfile.is_tarfile(fullPath):
                        self.__archs.append(scanArchFile(fullPath,"tar"))
                    if self.__COLLECT_FILES_CONTENT:
                        stat = os.stat(fullPath,follow_symlinks=False)
                        if stat.st_size > 250000000:
                            print("Large",self.__getSize__(stat.st_size))
                            self.__extra.append(tmp)
                            continue
                        else:
                            try:
                                self.__readContent__(tmp)
                            except UnicodeDecodeError as e:
                                self.errorEncountered("UnicodeDecodeError",e,fullPath)
                            except PermissionError as e:
                                self.errorEncountered("PermissionError",e,fullPath)
                    if tmp.totalHits() > 0:
                        self.hits["files"].append(tmp)
                        self.__hitFiles+=1
                    else:
                        self.__nullise__(tmp)
        self.switchingPhase()
        
    def __readExtra__(self):
        for ex in self.__extra:
            self.__readContent__(ex)
            if ex.totalHits() > 0:
                self.hits["files"].append(tmp)
                self.__hitFiles+=1
            else:
                self.__nullise__(ex)
                
    def __readContent__(self,tmp):
        with open(tmp.path,'rb') as f:
            tmp.contentHits+=self.__compare__(self.__searchBytes,f.read())
            
    def __waitForArchs__(self):
        while len(self.__archs)>0 and self.__SEARCH_STATUS <2:
            getarc = self.__archs.pop()
            if getarc.form == 'zip':
                arfile=zipfile.ZipFile(getarc.path,mode='r')
                zipyfile = self.__zippy__(arfile)
                self.__ziperate__(zipyfile)
                self.__nullise__(zipyfile)
            elif getarc.form == 'tar':
                arfile=tarfile.TarFile(name=getarc.path,mode='r')
                taryfile = self.__zippy__(arfile)
                self.__tarerate__(taryfile)
                self.__nullise__(taryfile)
            arfile.close()
        self.__checkDone__()
            
    def __nullise__(self,oj):
        del oj
        oj = None
        return True
    def __checkDone__(self):
        self.__endSearch__()
                
    def __endSearch__(self):
        self.__SEARCH_STATUS = 2
        self.endTime = time.time()
        self.endAction()
        
    def errorEncountered(self,errorType,errorMsg,offendingFile):
        print(errorType,errorMsg,offendingFile)
        
    class __zippy__:
        def __init__(self,oref):
            self.oref = oref
            
class normalFile:
    def __init__(self,path,filename):
        self.path =os.path.abspath(path)
        self.filename = filename
        self.titleHits = 0
        self.contentHits = 0
    def totalHits(self):
        return self.titleHits + self.contentHits

class archivedFile(normalFile):
    def __init__(self,path,filename,parent):
        super().__init__(path,os.path.basename(filename))
        self.parent = os.path.abspath(parent)

class ArgumentError(RuntimeError):
    def __init__(self,arg):
        print(arg)

class scanArchFile:
    def __init__(self,path,form):
        self.path = path
        self.form = form
          
#dog=carrotSearch("C:/python","python")
