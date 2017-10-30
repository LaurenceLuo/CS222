#include "pfm.h"

bool FileExists(const string &fileName)
{
    struct stat stFileInfo;
    
    if(stat(fileName.c_str(), &stFileInfo) == 0) return true;
    else return false;
}

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
    if(_pf_manager){
        delete _pf_manager;
        _pf_manager=NULL;
    }
}


RC PagedFileManager::createFile(const string &fileName)
{
    if(FileExists(fileName)){
        cout << "File " << fileName << " already exists. Create fail!" << endl << endl;
        return -1;
    }
    else{
        ofstream fs(fileName);
        fs.close();
    }
    return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    if(!FileExists(fileName)){
        cout<<"File " << fileName << " not exists. Destroy fail!" << endl << endl;
        return -1;
    }
    else{
        if(remove(fileName.c_str())!=0){
            perror("Error deleting file");
            return -1;
        }
    }
    return 0;
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    fstream *fs=new fstream();
    fs->open(fileName.c_str(),fstream::in | fstream::out);
    if(!(*fs)){
        cout<<"File " << fileName << " not exists. Open fail!" << endl << endl;
        return -1;
    }
    fileHandle._openFile(fs);
    return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    fileHandle._closeFile();
    return 0;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
}

RC FileHandle::_openFile(fstream* file)
{
    if(!file)
        return -1;
    myFile=file;
    return 0;
}

RC FileHandle::_closeFile()
{
    if(!myFile->is_open()){
        cout<<"File not open. Close fail!" << endl << endl;
        return -1;
    }
    myFile->close();
    if(myFile)
        delete myFile;

    return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    if(myFile->is_open()){
        unsigned pages=getNumberOfPages();
        if(pageNum>pages){
            cout<<"Invalid input pageNum for read!" << endl << endl;
            return -1;
        }
        else{
            char* temp=(char* )data;
            long pos=pageNum*PAGE_SIZE;
            myFile->seekg(pos);
            if(!myFile->read (temp,PAGE_SIZE)){
                cout<<"Read fail!" << endl << endl;
                return -1;
            }
            readPageCounter++;
        }
    }
    else{
        cout<<"File not open! Read fail!" << endl << endl;
        return -1;
    }
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    if(myFile->is_open()){
        unsigned pages=getNumberOfPages();
        if(pageNum>pages-1){
            cout<<"Invalid input pageNum for write!" << endl << endl;
            return -1;
        }
        else{
            char* temp=const_cast<char*>(reinterpret_cast<const char*> (data));
            long pos=pageNum*PAGE_SIZE;
            myFile->seekp(pos);
            if(!myFile->write (temp,PAGE_SIZE)){
                cout<<"Write fail!" << endl << endl;
                return -1;
            }
            writePageCounter++;
        }
    }
    else{
        cout<<"File not open! Write fail!" << endl << endl;
        return -1;
    }
    return 0;
}


RC FileHandle::appendPage(const void *data)
{
    if(myFile->is_open()){
        char* temp=const_cast<char*>(reinterpret_cast<const char*> (data));
        long pos=myFile->tellp();
        myFile->seekp(pos);
        
        if(!myFile->write (temp,PAGE_SIZE)){
            cout<<"Append fail!" << endl << endl;
            return -1;
        }
        appendPageCounter++;
        temp=NULL;
    }
    else{
        cout<<"File not open! Append fail!" << endl << endl;
        return -1;
    }
    return 0;
}


unsigned FileHandle::getNumberOfPages()
{
    myFile->seekp(0, ios_base::end);
    long length=myFile->tellp();
    //cout<<"File size: "<<length<<" bytes."<<endl<<endl;
    return length/PAGE_SIZE;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount=readPageCounter;
    writePageCount=writePageCounter;
    appendPageCount=appendPageCounter;
    return 0;
}
