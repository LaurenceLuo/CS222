#include "rbfm.h"


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
    if(_rbf_manager){
        delete _rbf_manager;
        _rbf_manager=NULL;
    }
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager *pfm_manager=PagedFileManager::instance();
    int currDir=0;
    int nextDir=1;
    if(pfm_manager->createFile(fileName)!=0){
        cout<< "File " << fileName << " create fail!" << endl << endl;
        return -1;
    }
    FileHandle fileHandle;
    if(pfm_manager->openFile(fileName,fileHandle)!=0){
        cout<< "File " << fileName << " open fail!" << endl << endl;
        return -1;
    }
    makeDirectoryPage(fileHandle,currDir,nextDir);
    if(pfm_manager->closeFile(fileHandle)!=0){
        cout<< "File " << fileName << " close fail!" << endl << endl;
        return -1;
    }
    return 0;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager *pfm_manager=PagedFileManager::instance();
    if(pfm_manager->destroyFile(fileName)!=0){
        cout<< "File " << fileName << " destory fail!" << endl << endl;
        return -1;
    }
    return 0;
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager *pfm_manager=PagedFileManager::instance();
    if(pfm_manager->openFile(fileName,fileHandle)!=0){
        cout<< "File " << fileName << " open fail!" << endl << endl;
        return -1;
    }
    return 0;
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager *pfm_manager=PagedFileManager::instance();
    if(pfm_manager->closeFile(fileHandle)!=0){
        cout<< "File close fail!" << endl << endl;
        return -1;
    }
    return 0;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    char* page=new char[PAGE_SIZE];
    char* formattedRecord;
    int formattedRecordSize=formatRecord(recordDescriptor,data,formattedRecord);
    //cout<<"formattedRecordSize: "<<formattedRecordSize<<endl;
    rid.pageNum=getFreePage(fileHandle,formattedRecordSize);
    //cout<<"Page Num got: "<<rid.pageNum<<" fileHandle.getNumberOfPages(): "<<fileHandle.getNumberOfPages()<<endl;
    if(rid.pageNum==fileHandle.getNumberOfPages()){
        //fileHandle.appendPage(page);
        //makePageDirectory(fileHandle,rid.pageNum);
        rid.slotNum = 0;//slotNum starting from 0
        Slot slot;
        slot.offset=0;
        slot.length=formattedRecordSize;
        memcpy(page,formattedRecord,formattedRecordSize);
        memcpy(page+PAGE_SIZE-sizeof(Slot)-sizeof(DirDescription),&slot,sizeof(Slot));
        DirDescription dirDescription;
        dirDescription.slotCount=1;
        dirDescription.freeSpacePointer=slot.length;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
        fileHandle.appendPage(page);
        
        //cout<<"slot.lengthWrite: "<<slot.length<<" slot.slotNum: "<<rid.slotNum<<endl;
    }
    else{
        if(fileHandle.readPage(rid.pageNum,page)!=0){
            cout<<"readPage from insertRecord fail!"<<endl;
            return -1;
        }
        DirDescription dirDescription;
        //cout<<"Working"<<endl;
        memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
        Slot slot;
        slot.offset=dirDescription.freeSpacePointer;
        slot.length=formattedRecordSize;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(dirDescription.slotCount+1),&slot,sizeof(Slot));
        memcpy(page+dirDescription.freeSpacePointer, formattedRecord, formattedRecordSize);
        
        //update DirDescription
        rid.slotNum=dirDescription.slotCount;
        dirDescription.slotCount++;
        dirDescription.freeSpacePointer+=formattedRecordSize;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
        fileHandle.writePage(rid.pageNum,page);
        //cout<<"pageNumWrite: "<<rid.pageNum<<" slotNumWrite: "<<rid.slotNum<<endl;
    }
    //cout<<"rid.pageNum: "<<rid.pageNum<<" rid.slotNum: "<<rid.slotNum<<endl;
        //delete not implemented yet;
    delete []page;
    delete []formattedRecord;

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *page=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum,page)!=0){
        cout<<"read page from read record fail!"<<endl;
        return -1;
    }
    DirDescription dirDescription;
    memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
    Slot slot;
    memcpy(&slot,page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),sizeof(Slot));
    
    //cout<<"slot.length: "<<slot.length<<endl;
    if(slot.length==DELETE_MARK){
        cout<<"Record been deleted!"<<endl;
        return -1;
    }
    
    if(slot.length==TOMBSTONE){
        cout<<"read TOMBSTONE encountered!"<<endl;
        RID nextRid;
        memcpy(&nextRid,page+slot.offset,sizeof(RID));
        cout<<"New pageNum: "<<nextRid.pageNum<<", slotNum: "<<nextRid.slotNum<<endl;
        return readRecord(fileHandle,recordDescriptor,nextRid,data);
    }
    int fieldOffsetSize =recordDescriptor.size()*sizeof(short);
    int fieldTotalSize=sizeof(short)+fieldOffsetSize;
    int dataSize=slot.length-fieldTotalSize;
    
    char* formattedRecord=new char[slot.length];
    memcpy(formattedRecord,page+slot.offset,slot.length);
    char* oldData=new char[dataSize];
    memcpy(oldData,formattedRecord+fieldTotalSize,dataSize);
    
    short fieldOffset;
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    unsigned char* nullIndicator=(unsigned char *) malloc(nullIndicatorSize);
    memset(nullIndicator, 0, nullIndicatorSize);

    for(int i=0;i<recordDescriptor.size();i++){
        //cout<<"recordDescriptor.size(): "<<recordDescriptor.size()<<endl;
        memcpy(&fieldOffset,formattedRecord+sizeof(short)*(i+1),sizeof(short));
        if(fieldOffset==fieldTotalSize){//null Bit Found
            nullIndicator[0]+=1<<(7*nullIndicatorSize-i);
        }
    }
    
    memcpy(data,nullIndicator,nullIndicatorSize);
    memcpy((char*)data+nullIndicatorSize,oldData,dataSize);
    //cout<<"Rrid pageNum: "<<rid.pageNum<<" slotNum: "<<rid.slotNum<<endl;
    //printRecord(recordDescriptor,data);
    
    delete []page;
    delete []oldData;
    delete []formattedRecord;

    return 0;
}

RC RecordBasedFileManager::makeDirectoryPage(FileHandle &fileHandle,int &currDir, int &nextDir)
{
    //int totalPages=PAGE_SIZE/sizeof(Directory);
    char *page=new char[PAGE_SIZE];
    //cout<<"sizeof(DirPageInfo): "<<sizeof(DirPageInfo)<<endl;
    for(int i=1; i< DIR_NUM; i++){
        Directory directory;
        directory.pageNum = i+currDir*DIR_NUM;
        //cout<<"pageNum made: "<<directory.pageNum<<endl;
        directory.freeSpace = PAGE_SIZE-sizeof(DirDescription);
        memcpy(page + sizeof(Directory)*i , &directory , sizeof(Directory) );
    }
    DirPageInfo dirInfo;
    dirInfo.currDir=currDir;
    dirInfo.nextDir=nextDir;
    memcpy(page , &dirInfo , sizeof(DirPageInfo) );
    fileHandle.appendPage(page);
    delete []page;
    return 0;
}

int RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data){
    int recordSize=0;
    char *_data=(char*)data;
    
    int nullIndicatorBytes=getNullIndicatorSize(recordDescriptor);
    
    recordSize+=nullIndicatorBytes;
    //_data+=nullIndicatorBytes;
    
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        switch(i->type){
            case TypeInt:
                recordSize+=sizeof(int);
                break;
            case TypeReal:
                recordSize+=sizeof(float);
                break;
            case TypeVarChar:
                int length;
                memcpy(&length , _data+recordSize, sizeof(int));
                recordSize += sizeof(int);
                recordSize += length;
                break;
               
        }
    }
    return recordSize;
}

int RecordBasedFileManager::getNullIndicatorSize(const vector<Attribute> &recordDescriptor){
    return ceil((double)recordDescriptor.size()/CHAR_BIT);
}

int RecordBasedFileManager::formatRecord(const vector<Attribute> &recordDescriptor, const void *data, char * &formattedRecord) {
    char *_data=(char*)data;
    int offset=0;

    short fieldSize=(short)recordDescriptor.size();
    short* fieldOffset=new short[recordDescriptor.size()*sizeof(short)];
    
    int fieldOffsetSize =recordDescriptor.size()*sizeof(short);
    int fieldTotalSize=sizeof(short)+fieldOffsetSize;
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    //cout<<"nullIndicatorSize: "<<nullIndicatorSize<<endl;//4
    char* nullIndicator=new char[nullIndicatorSize];
    memcpy(nullIndicator, data , nullIndicatorSize);
    
    int j=0;
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        if( ((unsigned char*) nullIndicator)[j/CHAR_BIT] & (1<< (7*nullIndicatorSize-(j%CHAR_BIT) ) )){
            fieldOffset[j]=fieldTotalSize;
        }
        else{
            switch(i->type){
                case TypeInt:
                    offset+=sizeof(int);
                    break;
                case TypeReal:
                    offset+=sizeof(float);
                    break;
                case TypeVarChar:
                    int length;
                    memcpy( &length, _data+nullIndicatorSize+offset, sizeof(int));
                    offset += length + sizeof(int);
                    break;
            }
            fieldOffset[j]=fieldTotalSize+offset;
        }
        j++;
    }
    
    int originalRecordSize=getRecordSize(recordDescriptor,data)-nullIndicatorSize;
    formattedRecord=new char[originalRecordSize+fieldTotalSize];
    //cout<<"originalRecordSize: "<<fieldOffsetSize<<" fieldTotalSize: "<<fieldTotalSize<<endl;
    //cout<<"FormattedRecordLengthWrite: "<<fieldTotalSize+originalRecordSize<<endl;
    //char *test=new char[originalRecordSize+fieldTotalSize];
    //memcpy(test+1,_data+nullIndicatorSize,22);
    
    memcpy(formattedRecord,&fieldSize,sizeof(short));
    memcpy(formattedRecord+sizeof(short),fieldOffset,fieldOffsetSize);
    memcpy(formattedRecord+fieldTotalSize,_data+nullIndicatorSize,originalRecordSize);
    //cout<<"FormattingRecord: fieldTotalSize: "<<fieldTotalSize<<" originalRecordSize: "<<originalRecordSize<<endl;
    /*int nullIndicatorS=getNullIndicatorSize(recordDescriptor);
    unsigned char* nu=(unsigned char *) malloc(nullIndicatorS);
    memset(nu, 0, nullIndicatorS);
    memcpy(test,nu,sizeof(short));
    cout<<"Print here!!!!!!!!!!"<<endl;
    printRecord(recordDescriptor,test);
    cout<<endl;*/
    

    delete []fieldOffset;
    delete []nullIndicator;
    
    return(fieldTotalSize+originalRecordSize);
}

int RecordBasedFileManager::getFreePage(FileHandle &fileHandle, int formattedRecordSize){
    int dirIndex=0;
    int nextDir=1;
    char* page=new char[PAGE_SIZE];
    if(fileHandle.readPage(dirIndex,page)!=0){
        cout<<"read page from getFreePage fail!"<<endl;
        return -1;
    }
    
    while(true){
        for(int i=1;i<DIR_NUM;i++){
            char *directory=new char[sizeof(Directory)];
            memcpy(directory,page+i*sizeof(Directory),sizeof(Directory));
            if(((Directory*)directory)->freeSpace >= formattedRecordSize+sizeof(Slot)+sizeof(char)){
                int pageNum=((Directory*)directory)->pageNum;
                ((Directory*)directory)->freeSpace-=(formattedRecordSize+sizeof(Slot)+sizeof(char));
                memcpy(page+i*sizeof(Directory),directory,sizeof(Directory));
                fileHandle.writePage(dirIndex*DIR_NUM,page);
                delete []directory;
                delete []page;

                return pageNum;
            }
        }
        dirIndex++;
        nextDir++;
        
        if(dirIndex*DIR_NUM==fileHandle.getNumberOfPages()){
            cout<<"Create new Directory Page."<<endl;
            makeDirectoryPage(fileHandle,dirIndex,nextDir);
            if(fileHandle.readPage(dirIndex*DIR_NUM,page)!=0){
                cout<<"readPage from getFreePage fail!"<<endl;
                cout<<"dirIndex*DIR_NUM: "<<dirIndex*DIR_NUM<<" fileHandle.getNumberOfPages(): "<<fileHandle.getNumberOfPages()<<" formattedRecordSize: "<<formattedRecordSize<<endl;
                return -1;
            }
        }
        else{
            if(fileHandle.readPage(dirIndex*DIR_NUM,page)!=0){
                cout<<"readPage from getFreePage fail!"<<endl;
                cout<<"dirIndex*DIR_NUM: "<<dirIndex*DIR_NUM<<" fileHandle.getNumberOfPages(): "<<fileHandle.getNumberOfPages()<<" formattedRecordSize: "<<formattedRecordSize<<endl;
                return -1;
            }

        }
    }
    delete []page;
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    char *_data=(char*)data;
    //_data+=byteForNullsIndicator;
    
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    char* nullIndicator=new char[nullIndicatorSize];
    memcpy(nullIndicator, data , nullIndicatorSize);
    int offset=0;
    offset+=nullIndicatorSize;
    
    int j=0;
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        if( ((unsigned char*) nullIndicator)[j/CHAR_BIT] & (1<< (7*nullIndicatorSize-(j%CHAR_BIT) ) ))
            cout<<i->name<<": NULL"<<"\t";
        else{
        switch(i->type){
            case TypeInt:
            cout<<i->name<<": "<<(*(int*)_data)<<"\t";
                offset+=sizeof(int);
                _data+=sizeof(int);
                //cout<<"offset: "<<offset<<endl;
                break;
            case TypeReal:
                cout<<i->name<<": "<<(*(float*)_data)<<"\t";
                offset+=sizeof(float);
                _data+=sizeof(float);
                //cout<<"offset: "<<offset<<endl;
                break;
            case TypeVarChar:
                cout<<i->name<<": ";
                //unsigned length=i->length;
                int len;
                memcpy( &len, (char*)data+offset, sizeof(int));
                offset+=sizeof(int);
                _data += offset;
                for(int j = 0; j < len; j++){
                    cout<<*_data;
                    _data++;
                }
                cout<<"\t";
                offset+=len;
                //cout<<"offset: "<<offset<<endl;
                //_data+=offset;

                break;
        }
        }
        j++;
    }
    cout<<endl;
    delete []nullIndicator;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    char *page=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum,page) != 0 ){
        cout<<"read page from deleteRecord fail!"<<endl;
        return -1;
    }
    DirDescription dirDescription;
    memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
    
    if(dirDescription.slotCount==0){
        cout<<"Empty page!"<<endl;
        return -1;
    }
    //cout<<"dirDescription.slotCount: "<<dirDescription.slotCount<<endl;
    Slot *slots=new Slot[dirDescription.slotCount];
    for(int i=0;i<dirDescription.slotCount;i++)
        memcpy(&slots[i],page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),sizeof(Slot));
    
    if(slots[rid.slotNum].length==TOMBSTONE){
        cout<<"Delete TOMBSTONE encountered!"<<endl;
        RID nextRid;
        memcpy(&nextRid,page+slots[rid.slotNum].offset,sizeof(RID));
        return deleteRecord(fileHandle,recordDescriptor,nextRid);
    }
    
    if(slots[rid.slotNum].length==DELETE_MARK){
        cout<<"Record already been deleted!"<<endl;
        return -1;
    }
    //cout<<"rid.slotNum].length: "<<slots[rid.slotNum].length<<endl;
    //int recordSize=slots[rid.slotNum].length;
    int lengthToShift=0;
    for(int i=0;i<dirDescription.slotCount;i++){
        if(slots[i].offset>slots[rid.slotNum].offset){
            slots[i].offset-=slots[rid.slotNum].length;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),&slots[i],sizeof(Slot));
            lengthToShift+=slots[i].length;
        }
    }

    char* shiftContent=new char[lengthToShift];
    memcpy(shiftContent,page+slots[rid.slotNum].offset+slots[rid.slotNum].length,lengthToShift);
    memcpy(page+slots[rid.slotNum].offset,shiftContent,lengthToShift);
    delete []shiftContent;
    //cout<<"oldfreeSpacePointer: "<<dirDescription.freeSpacePointer<<endl;
    dirDescription.freeSpacePointer-=slots[rid.slotNum].length;
    memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
    //cout<<"freeSpacePointer: "<<dirDescription.freeSpacePointer<<endl;
    
    //Update Directory Page
    if(updateDirectoryPage(fileHandle,rid,slots[rid.slotNum].length)!=0){
        cout<<"Update Directory Page fail!"<<endl;
        return -1;
    }
    
    slots[rid.slotNum].length=DELETE_MARK;
    slots[rid.slotNum].offset=dirDescription.freeSpacePointer;
    memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),&slots[rid.slotNum],sizeof(Slot));
    
    if(fileHandle.writePage(rid.pageNum,page)!=0){
        cout<<"Write Page from deleteRecord fail!"<<endl;
        return -1;
    }
    delete []slots;
    delete []page;

    return 0;
}

RC RecordBasedFileManager::updateDirectoryPage(FileHandle &fileHandle, const RID &rid, short spaceToAdd){
    char* dirPage=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum/DIR_NUM,dirPage)!=0){
        cout<<"Read Directory Page from deleteRecord fail!"<<endl;
        return -1;
    }
    Directory directory;
    memcpy(&directory,dirPage+(rid.pageNum%DIR_NUM)*sizeof(Directory),sizeof(Directory));
    directory.freeSpace+=spaceToAdd;
    memcpy(dirPage+(rid.pageNum%DIR_NUM)*sizeof(Directory),&directory,sizeof(Directory));
    if(fileHandle.writePage((rid.pageNum/DIR_NUM)*DIR_NUM,dirPage)!=0){
        cout<<"Write Directory Page from deleteRecord fail!"<<endl;
        return -1;
    }
    delete []dirPage;
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){
    char *page=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum,page) != 0 ){
        cout<<"read page from updateRecord fail!"<<endl;
        return -1;
    }
    DirDescription dirDescription;
    memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
    
    Slot *slots=new Slot[dirDescription.slotCount];
    for(int i=0;i<dirDescription.slotCount;i++)
        memcpy(&slots[i],page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),sizeof(Slot));
    
    if(slots[rid.slotNum].length==DELETE_MARK){
        cout<<"Deleted record cannot be updated!"<<endl;
        return -1;
    }
    
    if(slots[rid.slotNum].length==TOMBSTONE){
        cout<<"update TOMBSTONE encountered!"<<endl;
        RID nextRid;
        memcpy(&nextRid,page+slots[rid.slotNum].offset,sizeof(RID));
        return updateRecord(fileHandle,recordDescriptor,data,nextRid);
    }
    
    char* formattedNewRecord;//to delete
    int newRecordSize=formatRecord(recordDescriptor,data,formattedNewRecord);
    int sizeDiff=slots[rid.slotNum].length-newRecordSize;
    if(sizeDiff==0){
        //cout<<"CASE1!!!!!"<<endl;
        memcpy(page+slots[rid.slotNum].offset,formattedNewRecord,newRecordSize);
        if(fileHandle.writePage(rid.pageNum,page)!=0){
            cout<<"Write Page from updateRecord fail!"<<endl;
            return -1;
        }
    }
    else if(sizeDiff>0){
        //cout<<"CASE2!!!!!"<<endl;
        int sizeDiff=slots[rid.slotNum].length-newRecordSize;
        int lengthToShift=0;
        for(int i=0;i<dirDescription.slotCount;i++){
            if(slots[i].offset>slots[rid.slotNum].offset){
                slots[i].offset-=sizeDiff;
                memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),&slots[i],sizeof(Slot));
                lengthToShift+=slots[i].length;
            }
        }
        
        char* shiftContent=new char[lengthToShift];
        memcpy(shiftContent,page+slots[rid.slotNum].offset+slots[rid.slotNum].length,lengthToShift);
        memcpy(page+slots[rid.slotNum].offset+newRecordSize,shiftContent,lengthToShift);
        delete []shiftContent;
        
        dirDescription.freeSpacePointer-=sizeDiff;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
        
        memcpy(page+slots[rid.slotNum].offset,formattedNewRecord,newRecordSize);
        
        //Update Directory Page
        if(updateDirectoryPage(fileHandle,rid,sizeDiff)!=0){
            cout<<"Update Directory Page fail!"<<endl;
            return -1;
        }
        slots[rid.slotNum].length=newRecordSize;
        //slots[rid.slotNum].offset=dirDescription.freeSpacePointer;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),&slots[rid.slotNum],sizeof(Slot));
        
        if(fileHandle.writePage(rid.pageNum,page)!=0){
            cout<<"Write Page from deleteRecord fail!"<<endl;
            return -1;
        }
    }
    else{
        sizeDiff*=-1;
        if(PAGE_SIZE-dirDescription.freeSpacePointer-sizeof(DirDescription)-sizeof(Slot)*dirDescription.slotCount>sizeDiff){//enough space for larger record
            //cout<<"CASE3!!!!!"<<endl;
            dirDescription.freeSpacePointer-=sizeDiff;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
            
            memcpy(page+slots[rid.slotNum].offset,formattedNewRecord,newRecordSize);
            
            //Update Directory Page
            if(updateDirectoryPage(fileHandle,rid,sizeDiff)!=0){
                cout<<"Update Directory Page fail!"<<endl;
                return -1;
            }
            slots[rid.slotNum].length=newRecordSize;
            //slots[rid.slotNum].offset=dirDescription.freeSpacePointer;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),&slots[rid.slotNum],sizeof(Slot));
            
            if(fileHandle.writePage(rid.pageNum,page)!=0){
                cout<<"Write Page from deleteRecord fail!"<<endl;
                return -1;
            }

        }
        else{
            //cout<<"Case4!!!!!"<<endl;
            //cout<<"slots[rid.slotNum].length: "<<slots[rid.slotNum].length<<endl;
            RID newRid;
            if(insertRecord(fileHandle,recordDescriptor,data,newRid)!=0){
                cout<<"Insert Record in updateRecord fail!"<<endl;
                return -1;
            }
            //cout<<"New pageNum: "<<newRid.pageNum<<", slotNum: "<<newRid.slotNum<<endl;
            int lengthDiff=slots[rid.slotNum].length-sizeof(RID);
            int lengthToShift=0;
            for(int i=0;i<dirDescription.slotCount;i++){
                if(slots[i].offset>slots[rid.slotNum].offset){
                    slots[i].offset-=lengthDiff;
                    memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),&slots[i],sizeof(Slot));
                    lengthToShift+=slots[i].length;
                }
            }
            
            char* shiftContent=new char[lengthToShift];
            memcpy(shiftContent,page+slots[rid.slotNum].offset+slots[rid.slotNum].length,lengthToShift);
            memcpy(page+slots[rid.slotNum].offset+lengthDiff,shiftContent,lengthToShift);
            delete []shiftContent;
            
            dirDescription.freeSpacePointer-=lengthDiff;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
            
            //memcpy(page+slots[rid.slotNum].offset,formattedNewRecord,newRecordSize);
            
            //Update Directory Page
            if(updateDirectoryPage(fileHandle,rid,lengthDiff)!=0){
                cout<<"Update Directory Page fail!"<<endl;
                return -1;
            }

            memcpy(page+slots[rid.slotNum].offset,&newRid,sizeof(RID));
            
            slots[rid.slotNum].length=TOMBSTONE;
            //slots[rid.slotNum].offset=dirDescription.freeSpacePointer;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),&slots[rid.slotNum],sizeof(Slot));
            //cout<<"HERE????"<<endl;
            if(fileHandle.writePage(rid.pageNum,page)!=0){
                cout<<"Write Page from deleteRecord fail!"<<endl;
                return -1;
            }

        }
    }
    
    delete []formattedNewRecord;
    delete []slots;
    delete []page;
    return 0;
}

