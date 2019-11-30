// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <string>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "src/env.h"

namespace bitweaving {

bool Env::IsFileExist(const std::string &filename) {
    return access(filename.c_str(), F_OK) == 0;
}

bool Env::IsDirectoryExist(const std::string &path) {
    struct stat st;
    return stat(path.c_str(), &st) == 0;
}

bool Env::IsDirectory(const std::string &path) {
    struct stat st;
    stat(path.c_str(), &st);
    return S_ISDIR(st.st_mode);
}

Status Env::FilePath(const std::string &directory, const std::string &file,
                     std::string &path) {
    if (file.find('/') != std::string::npos) {
        return Status::InvalidArgument("file name contains '/'.");
    }
    if (directory.length() == 0) {
        return Status::InvalidArgument("empty directory name.");
    }
    if (directory[directory.length() - 1] == '/') {
        path = directory + file;
    } else {
        path = directory + "/" + file;
    }
    return Status::OK();
}

Status Env::CreateDirectory(const std::string &path) {
    if (IsDirectoryExist(path)) {
        return Status::OK();
    }
#ifdef __MINGW64__
    if (mkdir(path.c_str()) != 0) {
#else
        if (mkdir(path.c_str(), 0755) != 0) {
#endif
        return Status::IOError(path, errno);
    }
    return Status::OK();
}

Status Env::DeleteDirectory(const std::string &path) {
    Status status;
    status = ClearDirectory(path);
    if (!status.IsOk())
        return status;

    if (rmdir(path.c_str()) != 0) {
        return Status::IOError(path, errno);
    }
    return Status::OK();
}

Status Env::ClearDirectory(const std::string &path) {
    Status status;
    DIR *dir = opendir(path.c_str());
    if (dir == NULL) {
        return Status::IOError(path, errno);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0)
            continue;
        std::string entry_name(entry->d_name);
        std::string entry_path;
        FilePath(path, entry_name, entry_path);
        if (IsDirectory(entry_path)) {
            status = DeleteDirectory(entry_path);
            if (!status.IsOk()) {
                return status;
            }
        } else {
            status = DeleteFile(entry_path);
            if (!status.IsOk()) {
                return status;
            }
        }
    }
    closedir(dir);
    return Status::OK();
}

Status Env::GetDirectoryFiles(const std::string &path,
                              std::vector<std::string> *result) {
    result->clear();
    DIR *dir = opendir(path.c_str());
    if (dir == NULL) {
        return Status::IOError(path, errno);
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, ".", 1) == 0 || strncmp(entry->d_name, "..", 2) == 0)
            continue;
        std::string entry_name(entry->d_name);
        std::string entry_path;
        FilePath(path, entry_name, entry_path);
        if (!IsDirectory(entry_path)) {
            result->push_back(entry_name);
        }
    }
    closedir(dir);
    return Status::OK();
}

Status Env::CreateDirectoryPath(const std::string &path) {
    size_t start = 0;
    size_t pos = 0;
    Status status;
    while ((pos = path.find('/', start)) != std::string::npos) {
        if (pos != start) {
            if (!IsDirectoryExist(path.substr(0, pos))) {
                status = CreateDirectory(path.substr(0, pos));
                if (!status.IsOk()) {
                    return status;
                }
            }
        }
        start = pos + 1;
    }
    if (!IsDirectoryExist(path)) {
        status = CreateDirectory(path);
        if (!status.IsOk())
            return status;
    }
    return Status::OK();
}

Status Env::DeleteFile(const std::string &path) {
    if (unlink(path.c_str()) != 0) {
        return Status::IOError(path, errno);
    }
    return Status::OK();
}

}  // namespace bitweaving
