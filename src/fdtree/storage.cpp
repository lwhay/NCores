/*-------------------------------------------------------------------------
 *
 * storage.cpp
 *	  Storage manager interface routines
 *
 * The license is a free non-exclusive, non-transferable license to reproduce, 
 * use, modify and display the source code version of the Software, with or 
 * without modifications solely for non-commercial research, educational or 
 * evaluation purposes. The license does not entitle Licensee to technical support, 
 * telephone assistance, enhancements or updates to the Software. All rights, title 
 * to and ownership interest in the Software, including all intellectual property rights 
 * therein shall remain in HKUST.
 *
 * IDENTIFICATION
 *	  FDTree: storage.cpp,2010/04/06 yinan
 *
 *-------------------------------------------------------------------------
 */

#include "storage.h"
#include "error.h"

//data path for storing data
char DATAPATH[256] = "f:\\index";
char CONFIGPATH[256] = "f:\\index";

int file_get_new_fid() {
    static int curFileId = 100;
    return curFileId++;
}

void print_page(Page page) {
    printf("page: \n");
    Entry *data = PAGE_DATA(page, Entry);
    for (int i = 0; i < PAGE_NUM(page); i++)
        printf("%d, ", data[i].key);
    printf("\n");
}

FilePtr file_open(int fid, bool isExist) {
    char path[256];
    sprintf(path, "%s\\%d.dat", DATAPATH, fid);
#ifdef _MSVC_
    int openflag = isExist ? OPEN_EXISTING : OPEN_ALWAYS;

    unsigned int fflag = 0;
    fflag |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;

    HANDLE fhdl = CreateFileA(path,
                              GENERIC_READ | GENERIC_WRITE,
                              FILE_SHARE_READ,
                              NULL,
                              openflag,
                              fflag,
                              NULL);
    if (fhdl == INVALID_HANDLE_VALUE) {
        elog(ERROR, "ERROR: FileOpen failed (error=%d)\n", GetLastError());
        exit(1);
    }
    return fhdl;
#else
    if (isExist) {
        return fopen(path, "rb+");
    } else {
        return fopen(path, "wb+");
    }
#endif
}

int file_close(FilePtr fhdl) {
#ifdef _MSVC_
    if (CloseHandle(fhdl) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
    return 1;
#else
    return fclose(fhdl);
#endif
}

FilePtr file_reopen(int fid) {
    char path[256];
    sprintf(path, "%s//%d.dat", DATAPATH, fid);
#ifdef _MSVC_
    int openflag = OPEN_EXISTING;

    unsigned int fflag = 0;
    fflag |= FILE_FLAG_NO_BUFFERING | FILE_FLAG_WRITE_THROUGH;

    HANDLE hdl = CreateFileA(path,
                             GENERIC_READ,
                             FILE_SHARE_READ,
                             NULL,
                             openflag,
                             fflag,
                             NULL);
    if (hdl == INVALID_HANDLE_VALUE) {
        elog(ERROR, "ERROR: FileOpen failed (error=%d)\n", GetLastError());
        exit(1);
    }
    return hdl;
#else
    return fopen(path, "rb+");
#endif
}

void file_seek(FilePtr fhdl, long long offset) {
#ifdef _MSVC_
    LARGE_INTEGER seek;
    LARGE_INTEGER toss;
    seek.QuadPart = offset;
    seek.QuadPart = seek.QuadPart * BLKSZ;
    if (!SetFilePointerEx(fhdl, seek, &toss, FILE_BEGIN)) {
        elog(ERROR, "ERROR: SetFilePointerEx failed (error=%d)\n", GetLastError());
        exit(1);
    }
#else
    bigseek(fhdl, offset * BLKSZ, SEEK_SET);
#endif
}

bool file_trySeek(FilePtr fhdl, long long offset) {
#ifdef _MSVC_
    LARGE_INTEGER seek;
    LARGE_INTEGER toss;
    seek.QuadPart = offset;
    seek.QuadPart = seek.QuadPart * BLKSZ;
    if (!SetFilePointerEx(fhdl, seek, &toss, FILE_BEGIN))
        return false;
    else
        return true;
#else
    bigseek(fhdl, (bigint) offset * BLKSZ, SEEK_SET);
#endif
}

DWORD file_read(FilePtr fhdl, Page buffer, long num) {
#ifdef _MSVC_
    BOOL e;
    DWORD nread;
    e = ReadFile(fhdl, buffer, num, &nread, NULL);
    if (!e) {
        elog(ERROR, "ERROR: FileRead I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
    return nread;
#else
    return fread(buffer, num, 1, fhdl);
#endif
}

DWORD file_write(FilePtr fhdl, Page buffer, long num) {
#ifdef _MSVC_
    BOOL e;
    DWORD nread;
    e = WriteFile(fhdl, buffer, num, &nread, NULL);
    if (!e && nread != num) {
        elog(ERROR, "ERROR: FileWrite I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
    return nread;
#else
    if (!fwrite(buffer, num, 1, fhdl)) {
        printf("------------------------File I/O Error-------------------------------\n");
        printf("Can't read block.\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
    return num;
#endif
}

DWORD file_tryWrite(FilePtr fhdl, Page buffer, long num) {
#ifdef _MSVC_
    BOOL e;
    DWORD nread;
    e = WriteFile(fhdl, buffer, num, &nread, NULL);
    if (!e && nread != num) {
        return 0;
    }
    return nread;
#else
    if (!fwrite(buffer, num, 1, fhdl)) {
        printf("------------------------File I/O Error-------------------------------\n");
        printf("Can't read block.\n");
        printf("---------------------------------------------------------------------\n");
        exit(-1);
    }
    return num;
#endif
}

void file_flush(FilePtr fhdl) {
#ifdef _MSVC_
    BOOL e;
    e = FlushFileBuffers(fhdl);
    if (!e) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
#else
    fflush(fhdl);
#endif
}

void file_delete(FilePtr fhdlr, FilePtr fhdls, int fid) {
    char path[256];
    sprintf(path, "%s//%d.dat", DATAPATH, fid);
#ifdef _MSVC_
    if (CloseHandle(fhdlr) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }

    if (CloseHandle(fhdls) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }

    if (DeleteFile(path) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
#else
    file_close(fhdlr);
    file_close(fhdls);
    remove(path);
#endif
}

void file_delete(FilePtr fhdl, int fid) {
    char path[256];
    sprintf(path, "%s//%d.dat", DATAPATH, fid);
#ifdef _MSVC_
    if (CloseHandle(fhdl) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }

    if (DeleteFile(path) == 0) {
        elog(ERROR, "ERROR: FileFlush I/O failed, winerr=%d\n", GetLastError());
        exit(1);
    }
#else
    file_close(fhdl);
    remove(path);
#endif
}

#ifndef _MSVC_

bool is_dir(const char *path) {
#ifdef __MINGW64__
    struct _stat64 statbuf;
    if (_stat64(path, &statbuf) ==0)
#else
    struct stat statbuf;
    if (stat(path, &statbuf) == 0)
#endif
    {
        return S_ISDIR(statbuf.st_mode) != 0;
    }
}

bool is_file(const char *path) {
#ifdef __MINGW64__
    struct _stat64 statbuf;
    if (_stat64(path, &statbuf) ==0)
#else
    struct stat statbuf;
    if (stat(path, &statbuf) == 0)
#endif
    return S_ISREG(statbuf.st_mode) != 0;
}

bool is_special_dir(const char *path) {
    return strcmp(path, ".") == 0 || strcmp(path, "..") == 0;
}

void get_file_path(const char *path, const char *file_name, char *file_path) {
    strcpy(file_path, path);
    if (file_path[strlen(path) - 1] != '/')
        strcat(file_path, "/");
    strcat(file_path, file_name);
}

void delete_file(const char *path) {
    DIR *dir;
    dirent *dir_info;
    char file_path[PATH_MAX];
    if (is_file(path)) {
        remove(path);
        return;
    }
    if (is_dir(path)) {
        if ((dir = opendir(path)) == NULL)
            return;
        while ((dir_info = readdir(dir)) != NULL) {
            get_file_path(path, dir_info->d_name, file_path);
            if (is_special_dir(dir_info->d_name))
                continue;
            delete_file(file_path);
            rmdir(file_path);
        }
    }
}

#endif

BOOL file_clearDataDir() {
#ifdef _MSVC_
    WIN32_FIND_DATA finddata;
    HANDLE hfind;
    int fid;
    const char *path = DATAPATH;
    char *pdir;

    pdir = new char[strlen(path) + 5];
    strcpy(pdir, path);
    if (path[strlen(path) - 1] != '\\')
        strcat(pdir, "\\*.*");
    else
        strcat(pdir, "*.*");

    hfind = FindFirstFile(pdir, &finddata);
    if (hfind == INVALID_HANDLE_VALUE)
        return FALSE;

    delete[]pdir;
    do {
        if (sscanf(finddata.cFileName, "%d.dat", &fid) < 1)
            continue;

        pdir = new char[strlen(path) + strlen(finddata.cFileName) + 2];
        sprintf(pdir, "%s\\%s", path, finddata.cFileName);

        //if (strcmp(finddata.cFileName,".") == 0 || strcmp(finddata.cFileName,"..") == 0)
        //{
        //	RemoveDirectory(pdir);
        //	continue;
        //}
        if ((finddata.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) == 0)
            DeleteFile(pdir);

        delete[] pdir;
    } while (FindNextFile(hfind, &finddata));

    //if (RemoveDirectory(path))
    //	return TRUE;
    //else
    //	return FALSE;

    return TRUE;
#else
    delete_file(DATAPATH);
#endif
}

FILE *config_open(int fid, bool isExist) {
    char path[256];
    sprintf(path, "%s\\config%d.txt", CONFIGPATH, fid);
    if (isExist) {
        return fopen(path, "rb+");
    } else {
        return fopen(path, "wb+");
    }
}

void config_close(FILE *filep) {
    fflush(filep);
    fclose(filep);
}