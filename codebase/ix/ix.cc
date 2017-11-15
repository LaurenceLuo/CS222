
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->createFile(fileName);
}

RC IndexManager::destroyFile(const string &fileName)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->destroyFile(fileName);
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
    if(ixfileHandle.fileHandle._isOpen())
        return -1;
    if(pfm_manager->openFile(fileName,ixfileHandle.fileHandle)!=0){
        cout<< "File " << fileName << " open fail!" << endl << endl;
        return -1;
    }
    ixfileHandle.fileHandle.getNumberOfPages();
    return 0;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	PagedFileManager *pfm_manager=PagedFileManager::instance();
	return pfm_manager->closeFile(ixfileHandle.fileHandle);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    /*char* page=new char[PAGE_SIZE];
    int root=0;
    if(ixfileHandle.fileHandle.readPage(root,page)!=0){
        NodeDesc nodeDesc;
        cout<<"NodeDesc size: "<<sizeof(NodeDesc)<<endl;
        memcpy(page+PAGE_SIZE-sizeof(NodeDesc),&nodeDesc,sizeof(NodeDesc));
        if(ixfileHandle.fileHandle.writePage(root,page)!=0){
            cout<<"Error writing page in insertEntry, pageNum: "<<root<<endl;
        }
    }*/
    
    
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount = ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
    return 0;
}

