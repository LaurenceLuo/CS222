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
    PagedFileManager *pfm_manager=PagedFileManager::instance();//Memory leak?
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
    fileHandle.getNumberOfPages();
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
        if(fileHandle.appendPage(page)!=0){
            cout<<"appendPage from insertRecord fail!"<<endl;
            return -1;
        }
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
        slot.length=formattedRecordSize;
        slot.offset=dirDescription.freeSpacePointer;
        int slotIndex=0;
        Slot* slots=new Slot[dirDescription.slotCount];
        for(int i=0;i<dirDescription.slotCount;i++){
            memcpy(&slots[i],page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),sizeof(Slot));
            if(slots[i].length==DELETE_MARK){
                slotIndex=i;
                slot.offset=slots[i].offset;
                break;
            }
            slotIndex++;
        }
        delete []slots;
        //slot.offset=dirDescription.freeSpacePointer;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(slotIndex+1),&slot,sizeof(Slot));
        memcpy(page+dirDescription.freeSpacePointer, formattedRecord, formattedRecordSize);

        
        //update DirDescription
        rid.slotNum=slotIndex;
        dirDescription.slotCount++;
        dirDescription.freeSpacePointer+=formattedRecordSize;
        memcpy(page+PAGE_SIZE-sizeof(DirDescription),&dirDescription,sizeof(DirDescription));
        if(fileHandle.writePage(rid.pageNum,page)!=0){
            cout<<"writePage from insertRecord fail!"<<endl;
            return -1;
        }
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
    //cout<<"rid.pageNum: "<<rid.pageNum<<" rid.slotNum: "<<rid.slotNum<<endl;
    //cout<<"slot.offset: "<<slot.offset<<" slot.length: "<<slot.length<<endl;
    if(slot.length==DELETE_MARK){
        cout<<"Record been deleted!"<<endl;
        return -1;
    }
    //cout<<"Here??"<<endl;
    if(slot.length==TOMBSTONE){
        cout<<"read TOMBSTONE encountered!"<<endl;
        RID nextRid;
        memcpy(&nextRid,page+slot.offset,sizeof(RID));
        //cout<<"New pageNum: "<<nextRid.pageNum<<", slotNum: "<<nextRid.slotNum<<endl;
        return readRecord(fileHandle,recordDescriptor,nextRid,data);
    }
    int fieldOffsetSize =recordDescriptor.size()*sizeof(short);
    int fieldTotalSize=sizeof(short)+fieldOffsetSize;
    int dataSize=slot.length-fieldTotalSize;
    //cout<<"slot.length: "<<slot.length<<" fieldTotalSize: "<<fieldTotalSize<<endl;
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
            nullIndicator[i/CHAR_BIT]+=1<<(7-i%CHAR_BIT);
        }
    }
    
    memcpy(data,nullIndicator,nullIndicatorSize);
    memcpy((char*)data+nullIndicatorSize,oldData,dataSize);
    //cout<<"Rrid pageNum: "<<rid.pageNum<<" slotNum: "<<rid.slotNum<<endl;
    //printRecord(recordDescriptor,data);
    
    delete []page;
    delete []oldData;
    delete []formattedRecord;
    free(nullIndicator);

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
    //int nullIndicatorBytes=getNullIndicatorSize(recordDescriptor);
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    char* nullIndicator=new char[nullIndicatorSize];
    memcpy(nullIndicator, data , nullIndicatorSize);
    recordSize+=nullIndicatorSize;
    
    //_data+=nullIndicatorBytes;
    int j=0;
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        if( ((unsigned char*) nullIndicator)[j/CHAR_BIT] & (1<< (7-(j%CHAR_BIT) ) )){
            //cout<<"NULL bit here! "<<j<<endl;
        }
        else{
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
                    //cout<<"recordSize: "<<recordSize<<" length: "<<length<<endl;
                    recordSize += sizeof(int);
                    recordSize += length;
                    break;

            }
        }
        j++;
    }
    delete []nullIndicator;
    //cout<<"Record Size: "<<recordSize<<endl;
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
    
    //int fieldOffsetSize =recordDescriptor.size()*sizeof(short);
    int fieldTotalSize=sizeof(short)+recordDescriptor.size()*sizeof(short);
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    //cout<<"nullIndicatorSize: "<<nullIndicatorSize<<endl;//4
    char* nullIndicator=new char[nullIndicatorSize];
    memcpy(nullIndicator, data , nullIndicatorSize);
    
    int j=0;
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        if( ((unsigned char*) nullIndicator)[j/CHAR_BIT] & (1<< (7-j%CHAR_BIT ) )){
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
    //cout<<"fieldTotalSize: "<<fieldTotalSize<<" originalRecordSize: "<<originalRecordSize<<endl;
    formattedRecord=new char[originalRecordSize+fieldTotalSize];
    //cout<<"originalRecordSize: "<<fieldOffsetSize<<" fieldTotalSize: "<<fieldTotalSize<<endl;
    //cout<<"FormattedRecordLengthWrite: "<<fieldTotalSize+originalRecordSize<<endl;
    //char *test=new char[originalRecordSize+fieldTotalSize];
    //memcpy(test+1,_data+nullIndicatorSize,22);
    
    memcpy(formattedRecord,&fieldSize,sizeof(short));
    memcpy(formattedRecord+sizeof(short),fieldOffset,recordDescriptor.size()*sizeof(short));
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
            else{
                delete []directory;
            }
        }
        dirIndex++;
        nextDir++;
        
        if(dirIndex*DIR_NUM==fileHandle.getNumberOfPages()){
            cout<<"Create new Directory Page."<<endl;
            makeDirectoryPage(fileHandle,dirIndex,nextDir);
            if(fileHandle.readPage(dirIndex*DIR_NUM,page)!=0){
                cout<<"readPage from getFreePage fail!"<<endl;
                //cout<<"dirIndex*DIR_NUM: "<<dirIndex*DIR_NUM<<" fileHandle.getNumberOfPages(): "<<fileHandle.getNumberOfPages()<<" formattedRecordSize: "<<formattedRecordSize<<endl;
                return -1;
            }
        }
        else{
            if(fileHandle.readPage(dirIndex*DIR_NUM,page)!=0){
                cout<<"readPage from getFreePage fail!"<<endl;
                //cout<<"dirIndex*DIR_NUM: "<<dirIndex*DIR_NUM<<" fileHandle.getNumberOfPages(): "<<fileHandle.getNumberOfPages()<<" formattedRecordSize: "<<formattedRecordSize<<endl;
                return -1;
            }

        }
    }
    delete []page;
    return -1;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    char *_data=(char*)data;
    
    int nullIndicatorSize=getNullIndicatorSize(recordDescriptor);
    char* nullIndicator=new char[nullIndicatorSize];
    memcpy(nullIndicator, data , nullIndicatorSize);
    int offset=0;
    offset+=nullIndicatorSize;
    _data+=nullIndicatorSize;
    
    int j=0;
    for(vector<Attribute>::const_iterator i=recordDescriptor.begin();i!=recordDescriptor.end();i++){
        if( ((unsigned char*) nullIndicator)[j/CHAR_BIT] & (1<< (7-(j%CHAR_BIT) ) )){
            cout<<j<<" "<<i->name<<": NULL"<<"\t";
        }
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
                    _data += sizeof(int);
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
    delete []nullIndicator;
    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
    char *page=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum,page) != 0 ){
        cout<<"read page from deleteRecord fail!"<<endl;
        return -1;
    }
    cout << "read suc!" <<endl;
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
    //cout<<"rid.slotNum.length: "<<slots[rid.slotNum].length<<endl;
    //int recordSize=slots[rid.slotNum].length;

    for(int i=0;i<dirDescription.slotCount;i++){
        if(slots[i].offset>slots[rid.slotNum].offset){
            slots[i].offset-=slots[rid.slotNum].length;
            memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),&slots[i],sizeof(Slot));
            //cout << "slot.length: " << slots[i].length << endl;
        }
    }

    int lengthToShift = 0;
    //cout << "freeSpacePointer: " << dirDescription.freeSpacePointer << endl;
    //cout << "slots[rid.slotNum].offset: " << slots[rid.slotNum].offset << endl;
    //cout << "slots[rid.slotNum].length: " << slots[rid.slotNum].length << endl;
    lengthToShift = dirDescription.freeSpacePointer - (slots[rid.slotNum].offset + slots[rid.slotNum].length);
    //cout << "lengthToShift: " << lengthToShift << endl;
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

            int lengthToShift=0;//shift forward
            for(int i=0;i<dirDescription.slotCount;i++){
                if(slots[i].offset>slots[rid.slotNum].offset){
                    slots[i].offset+=sizeDiff;
                    memcpy(page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(i+1),&slots[i],sizeof(Slot));
                    lengthToShift+=slots[i].length;
                }
            }

            char* shiftContent=new char[lengthToShift];
            memcpy(shiftContent,page+slots[rid.slotNum].offset+slots[rid.slotNum].length,lengthToShift);
            memcpy(page+slots[rid.slotNum].offset+newRecordSize,shiftContent,lengthToShift);
            delete []shiftContent;

            //cout<<"old freeSpacePointer: "<<dirDescription.freeSpacePointer<<" sizeDiff: "<<sizeDiff<<endl;
            dirDescription.freeSpacePointer+=sizeDiff;
            //cout<<"dirDescription.freeSpacePointer: "<<dirDescription.freeSpacePointer<<" sizeDiff: "<<sizeDiff<<endl;
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

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){
    char *page=new char[PAGE_SIZE];
    if(fileHandle.readPage(rid.pageNum,page) != 0 ){
        cout<<"read page from updateRecord fail!"<<endl;
        return -1;
    }

    DirDescription dirDescription;
    memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));

    Slot slot;
    memcpy(&slot,page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(rid.slotNum+1),sizeof(Slot));

    char* record=new char[slot.length];
    memcpy(record,page+slot.offset,slot.length);

    short* fieldOffset=new short[recordDescriptor.size()];

    int fieldOffsetSize =recordDescriptor.size()*sizeof(short);
    int fieldTotalSize=sizeof(short)+fieldOffsetSize;
    for(int i=0;i<recordDescriptor.size();i++)
        memcpy(&fieldOffset[i],record+sizeof(short)*i,sizeof(short));

    int newNullIndicatorSize=ceil((double)1/CHAR_BIT);
    unsigned char* newNullIndicator=(unsigned char *) malloc(newNullIndicatorSize);
    memset(newNullIndicator, 0, newNullIndicatorSize);

    int offset=fieldTotalSize;
    for(int i=0;i<recordDescriptor.size();i++){
        if(fieldOffset[i]==fieldTotalSize){
            newNullIndicator[i/CHAR_BIT]+=1<<(7-i%CHAR_BIT);
            break;
        }
        switch(recordDescriptor[i].type){
            case TypeInt:
                if(recordDescriptor[i].name.compare(attributeName)==0){
                    memcpy(data,newNullIndicator,newNullIndicatorSize);
                    memcpy((char*)data+newNullIndicatorSize,record+offset,sizeof(int));
                    //cout<<"DATA: "<<*(int*)data<<endl;
                    return 0;
                }
                offset+=sizeof(int);
                break;
            case TypeReal:
                if(recordDescriptor[i].name.compare(attributeName)==0){
                    memcpy(data,newNullIndicator,newNullIndicatorSize);
                    memcpy((char*)data+newNullIndicatorSize,record+offset,sizeof(float));
                    return 0;
                }
                offset+=sizeof(float);
                break;
            case TypeVarChar:
                int length;
                memcpy( &length, record+offset, sizeof(int));
                if(recordDescriptor[i].name.compare(attributeName)==0){
                    memcpy(data,newNullIndicator,newNullIndicatorSize);
                    memcpy((char*)data+newNullIndicatorSize,record+offset+sizeof(int),length);
                    return 0;
                }
                offset+=sizeof(int);
                offset+=length;
                break;
        }
    }
    delete []fieldOffset;
    delete []record;
    delete []page;
    cout<<"No such attribute!"<<endl;
    return -1;
}

RC RBFM_ScanIterator::initialization(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames){
    _fileHandle=fileHandle;
    _fileHandle.getNumberOfPages();
    _recordDescriptor=recordDescriptor;
    _conditionAttribute=conditionAttribute;
    _compOp=compOp;
    _value=(char*)value;
    _attributeNames=attributeNames;
    _rid.pageNum=1;
    _rid.slotNum=0;
    return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator){
    return rbfm_ScanIterator.initialization(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames);
}

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    RecordBasedFileManager *_rbfm=RecordBasedFileManager::instance();
    char *page=new char[PAGE_SIZE];
    if(_rid.pageNum>_fileHandle.getNumberOfPages()){
        cout<<"Invalid pageNum: "<<_rid.pageNum<<" for getNextRecord!"<<endl;
        return -1;
    }
    if(_fileHandle.readPage(_rid.pageNum,page)!=0){
        cout<<"readPage from getNextRecord fail!"<<" PageNum: "<<endl;
        return -1;
    }

    bool found=false;
    DirDescription dirDescription;
    memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
    while(!found){
        //cout<<"_rid.slotNum: "<<_rid.slotNum<<" dirDescription.slotCount: "<<dirDescription.slotCount<<endl;
        if(_rid.slotNum+1>dirDescription.slotCount){
            _rid.slotNum=0;
            _rid.pageNum++;
            if(_rid.pageNum%DIR_NUM==0)
                _rid.pageNum++;
            if(_rid.pageNum>_fileHandle.getNumberOfPages()){
                cout<<"Approach file end. "<<endl;
                return RBFM_EOF;
            }
            else
                _fileHandle.readPage(_rid.pageNum,page);
            memcpy(&dirDescription,page+PAGE_SIZE-sizeof(DirDescription),sizeof(DirDescription));
        }
        Slot slot;
        memcpy(&slot,page+PAGE_SIZE-sizeof(DirDescription)-sizeof(Slot)*(_rid.slotNum+1),sizeof(Slot));

        int fieldOffsetSize =_recordDescriptor.size()*sizeof(short);
        int fieldTotalSize=sizeof(short)+fieldOffsetSize;
        int dataSize=slot.length-fieldTotalSize;
        int nullIndicatorSize=ceil((double)_recordDescriptor.size()/CHAR_BIT);
        char *record=new char[dataSize+nullIndicatorSize];

        if(_rbfm->readRecord(_fileHandle, _recordDescriptor, _rid, record)!=0){
            cout<<"ReadRecord from getNextRecord fail!"<<endl;
            return -1;
        }

        //unsigned char* nullIndicator=(unsigned char *) malloc(nullIndicatorSize);
        //memcpy(nullIndicator,record,nullIndicatorSize);
        int offset=nullIndicatorSize;
        for(int j=0;j<_recordDescriptor.size();j++){
            if(_conditionAttribute.empty()){
                found=true;
                rid=_rid;
                //cout<<"Found: rid.pageNum: "<<rid.pageNum<<" rid.slotNum: "<<rid.slotNum<<endl;
                writeIntoData(record,_recordDescriptor,_attributeNames,data);
                break;
            }

            switch(_recordDescriptor[j].type){
                case TypeInt:
                    if(_recordDescriptor[j].name.compare(_conditionAttribute)==0){
                        //memcpy((int*)data,record+offset,sizeof(int));
                        int storedValue;
                        memcpy(&storedValue,record+offset,sizeof(int));
                        if(compareNum(&storedValue,_compOp,_value,_recordDescriptor[j].type)){
                            found=true;
                            rid=_rid;
                            writeIntoData(record,_recordDescriptor,_attributeNames,data);
                            break;
                        }
                    }
                    offset+=sizeof(int);
                    break;
                case TypeReal:
                    if(_recordDescriptor[j].name.compare(_conditionAttribute)==0){
                        float storedValue;
                        memcpy(&storedValue,record+offset,sizeof(float));
                        if(compareNum(&storedValue,_compOp,_value,_recordDescriptor[j].type)){
                            found=true;
                            rid=_rid;
                            writeIntoData(record,_recordDescriptor,_attributeNames,data);
                            break;
                        }
                    }
                    offset+=sizeof(float);
                    break;
                case TypeVarChar:
                    int length;
                    memcpy( &length, record+offset, sizeof(int));
                    if(_recordDescriptor[j].name.compare(_conditionAttribute)==0){
                        char *storedValue=new char[length];
                        memcpy(storedValue,record+offset+sizeof(int),length);
                        int _valurLength=(int)strlen((char*)_value);
                        if(compareVarChar(length,storedValue,_compOp,_valurLength,_value)){
                            found=true;
                            rid=_rid;
                            writeIntoData(record,_recordDescriptor,_attributeNames,data);
                            delete []storedValue;
                            break;
                        }
                        //compare(_value,compOp,value);
                        delete []storedValue;
                    }
                    offset+=sizeof(int)+length;
            }
        }
        delete []record;
        _rid.slotNum++;
    }
    delete []page;
    return 0;
}

bool RBFM_ScanIterator::compareNum(void* storedValue, const CompOp compOp, const void *valueToCompare, AttrType type){
    switch(compOp){
        case EQ_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue==*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue==*(float*)valueToCompare;
                default:
                    return true;
            }
        case LT_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue<*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue<*(float*)valueToCompare;
                default:
                    return true;
            }
        case LE_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue<=*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue<=*(float*)valueToCompare;
                default:
                    return true;
            }
        case GT_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue>*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue>*(float*)valueToCompare;
                default:
                    return true;
            }
        case GE_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue>=*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue>=*(float*)valueToCompare;
                default:
                    return true;
            }
        case NE_OP:
            switch(type){
                case TypeInt:
                    return *(int*)storedValue!=*(int*)valueToCompare;
                case TypeReal:
                    return *(float*)storedValue!=*(float*)valueToCompare;
                default:
                    return true;
            }
        case NO_OP:
            return true;
    }
    return 0;
}

bool RBFM_ScanIterator::compareVarChar(int &storedValue_len, void* storedValue, const CompOp compOp, int &valueToCompare_len, const void *valueToCompare){
    int sizeToCompare=storedValue_len;
    if(storedValue_len!=valueToCompare_len)
        sizeToCompare=min(storedValue_len,valueToCompare_len);
    switch(compOp){
        case EQ_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)==0;
        case LT_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)<0;
        case LE_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)<=0;
        case GT_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)>0;
        case GE_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)>=0;
        case NE_OP: return strncmp((char*)storedValue,(char*)valueToCompare,sizeToCompare)!=0;
        case NO_OP: return true;
    }
}

RC RBFM_ScanIterator::writeIntoData(void *returnedData, const vector<Attribute> &recordDescriptor, const vector<string> &attributeNames, void *data){
    int nullIndicatorSize=ceil((double)recordDescriptor.size()/CHAR_BIT);
    unsigned char* nullIndicator=(unsigned char *) malloc(nullIndicatorSize);
    memcpy(nullIndicator,returnedData,nullIndicatorSize);

    int newNullIndicatorSize=ceil((double)attributeNames.size()/CHAR_BIT);
    unsigned char* newNullIndicator=(unsigned char *) malloc(newNullIndicatorSize);
    memset(newNullIndicator, 0, newNullIndicatorSize);

    int returnedDataOffset;
    //cout<<"nullIndicatorSize: "<<nullIndicatorSize<<" newNullIndicatorSize: "<<newNullIndicatorSize<<endl;
    int dataOffset=newNullIndicatorSize;
    for(int i=0;i<attributeNames.size();i++){
        returnedDataOffset=nullIndicatorSize;
        for(int j=0;j<recordDescriptor.size();j++){
            if(_attributeNames[i].compare(_recordDescriptor[j].name)==0){
                if( nullIndicator[j/CHAR_BIT] & (1<< (7*nullIndicatorSize-(j%CHAR_BIT) ) )){
                    newNullIndicator[i/CHAR_BIT]+=1<<(7-i%CHAR_BIT);
                }
                else{
                    switch(recordDescriptor[j].type){
                        case TypeInt:
                            memcpy((char*)data+dataOffset,(char*)returnedData+returnedDataOffset,sizeof(int));
                            dataOffset+=sizeof(int);
                            //cout<<"dataOffset: "<<dataOffset<<endl;
                            break;
                        case TypeReal:
                            memcpy((char*)data+dataOffset,(char*)returnedData+returnedDataOffset,sizeof(float));
                            dataOffset+=sizeof(float);
                            //cout<<"dataOffset: "<<dataOffset<<endl;
                            break;
                        case TypeVarChar:
                            int length;
                            memcpy( &length, (char*)returnedData+returnedDataOffset, sizeof(int));
                            memcpy((char*)data+dataOffset,&length,sizeof(int));
                            memcpy((char*)data+dataOffset+sizeof(int),(char*)returnedData+returnedDataOffset+sizeof(int),length);
                            dataOffset+=sizeof(int)+length;
                            //cout<<"dataOffset: "<<dataOffset<<endl;
                            break;
                    }
                }
            }
            if(!( nullIndicator[j/CHAR_BIT] & (1<< (7-(j%CHAR_BIT) ) ))){
                switch(recordDescriptor[j].type){
                    case TypeInt:
                        returnedDataOffset+=sizeof(int);
                        //cout<<"returnedDataOffset: "<<returnedDataOffset<<endl;
                        break;
                    case TypeReal:
                        returnedDataOffset+=sizeof(float);
                        //cout<<"returnedDataOffset: "<<returnedDataOffset<<endl;
                        break;
                    case TypeVarChar:
                        int length;
                        memcpy( &length, (char*)returnedData+returnedDataOffset, sizeof(int));
                        returnedDataOffset+=sizeof(int)+length;
                        //cout<<"returnedDataOffset: "<<returnedDataOffset<<endl;
                        break;
                }
            }
        }
    }
    memcpy((char*)data,newNullIndicator,newNullIndicatorSize);
    free(nullIndicator);
    free(newNullIndicator);

    return 0;
}
