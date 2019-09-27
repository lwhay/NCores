// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include <cassert>
#include <map>
#include <queue>
#include <string>
#include <sstream>

#include "bitweaving/table.h"

#include "src/env.h"
#include "src/file.h"
#include "src/utility.h"

namespace bitweaving {

/**
 * @brief Structure for private members of Class Table.
 */
struct Table::Rep {
    ~Rep() {
        delete env;
    }

    /**
     * @brief The number of tuples in this table.
     */
    size_t num;
    /**
     * @brief The location of this table in file system.
     */
    std::string path;
    /**
     * @brief The Env object of this table.
     */
    Env *env;
    /**
     * @brief The options to control this table.
     */
    Options options;
    /**
     * @brief A mapping from column names to column objects.
     */
    std::map<std::string, Column *> map;
    /**
     * @brief A list of freed column IDs.
     */
    std::queue<ColumnId> free_column_ids;
};

Table::Table(const std::string &path, const Options &options) {
    rep_ = new Rep();
    rep_->num = 0;
    rep_->env = new Env();
    rep_->options = options;
    if (!options.in_memory)
        rep_->path = path;
}

Table::~Table() {
    delete rep_;
}

size_t Table::GetNumTuples() const {
    return rep_->num;
}

Status Table::Resize(size_t num_tuples) {
    Status status;
    if (rep_->num != num_tuples) {
        rep_->num = num_tuples;
        std::map<std::string, Column *>::iterator iter;
        for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
            status = iter->second->Resize(num_tuples);
            if (!status.IsOk()) {
                return status;
            }
        }
    }
    return Status::OK();
}

Status Table::Open() {
    Status status;

    if (rep_->options.in_memory)
        return Status::OK();

    status = rep_->env->CreateDirectoryPath(rep_->path);
    if (!status.IsOk()) {
        return status;
    }
    assert(rep_->env->IsDirectoryExist(rep_->path));

    std::string meta_file;
    status = rep_->env->FilePath(rep_->path, "meta.dat", meta_file);
    if (!status.IsOk()) {
        return status;
    }

    if (rep_->options.delete_exist_files) {
        status = rep_->env->ClearDirectory(rep_->path);
        if (!status.IsOk()) {
            return status;
        }
    }

    if (rep_->env->IsFileExist(meta_file)) {
        status = Load();
        if (!status.IsOk()) {
            return status;
        }
    }
    return Status::OK();
}

Status Table::Close() {
    if (rep_->options.in_memory)
        return Status::OK();

    return Status::OK();
}

Status Table::Load() {
    Status status;

    if (rep_->options.in_memory)
        return Status::OK();

    std::string meta_file;
    status = rep_->env->FilePath(rep_->path, "meta.dat", meta_file);
    if (!status.IsOk()) {
        return status;
    }

    status = LoadMetaFile(meta_file);
    if (!status.IsOk())
        return status;

    std::map<std::string, Column *>::iterator iter;
    for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
        std::string file_path;
        rep_->env->FilePath(rep_->path, iter->first + ".dat", file_path);

        iter->second->Load(file_path);
    }
    return Status::OK();
}

Status Table::Save() {
    Status status;

    if (rep_->options.in_memory)
        return Status::OK();

    std::string meta_file;
    status = rep_->env->FilePath(rep_->path, "meta.dat", meta_file);
    if (!status.IsOk()) {
        return status;
    }
    status = SaveMetaFile(meta_file);
    if (!status.IsOk())
        return status;

    std::map<std::string, Column *>::iterator iter;
    for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
        std::string file_path;
        rep_->env->FilePath(rep_->path, iter->first + ".dat", file_path);

        iter->second->Save(file_path);
    }
    return Status::OK();
}

Status Table::Delete() {
    Status status;
    if (rep_->options.in_memory)
        return Status::OK();

    return rep_->env->DeleteDirectory(rep_->path);
}

Status Table::LoadMetaFile(const std::string &filename) {
    Status status;
    SequentialReadFile file;

    if (rep_->options.in_memory)
        return Status::OK();

    status = file.Open(filename);
    if (!status.IsOk())
        return status;

    status = file.Read(reinterpret_cast<char *>(&rep_->num), sizeof(size_t));
    if (!status.IsOk())
        return status;

    size_t num_columns;
    status = file.Read(reinterpret_cast<char *>(&num_columns), sizeof(size_t));
    if (!status.IsOk())
        return status;

    for (size_t i = 0; i < num_columns; ++i) {
        size_t length;
        status = file.Read(reinterpret_cast<char *>(&length), sizeof(size_t));
        if (!status.IsOk())
            return status;
        char *buffer = new char[length + 1];
        buffer[length] = '\0';
        status = file.Read(reinterpret_cast<char *>(buffer), length);
        if (!status.IsOk())
            return status;

        // Add the column into the map.
        Column *column = new Column(this, i);
        std::pair<std::map<std::string, Column *>::iterator, bool> ret;
        ret = rep_->map.insert(std::pair<std::string, Column *>(buffer, column));
        if (ret.second == false) {
            return Status::UsageError("The column name exists in the table.");
        }

        delete buffer;
    }

    status = file.Close();
    if (!status.IsOk())
        return status;

    return Status::OK();
}

Status Table::SaveMetaFile(const std::string &filename) {
    Status status;
    SequentialWriteFile file;

    if (rep_->options.in_memory)
        return Status::OK();

    status = file.Open(filename);
    if (!status.IsOk())
        return status;

    status = file.Append(reinterpret_cast<const char *>(&rep_->num), sizeof(size_t));
    if (!status.IsOk())
        return status;

    size_t num_columns = rep_->map.size();
    status = file.Append(reinterpret_cast<const char *>(&num_columns), sizeof(size_t));
    if (!status.IsOk())
        return status;

    std::map<std::string, Column *>::iterator iter;
    for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
        size_t length = iter->first.length();
        status = file.Append(reinterpret_cast<const char *>(&length), sizeof(size_t));
        if (!status.IsOk())
            return status;
        status = file.Append(reinterpret_cast<const char *>(iter->first.c_str()), length);
        if (!status.IsOk())
            return status;
    }

    status = file.Flush();
    if (!status.IsOk())
        return status;

    status = file.Close();
    if (!status.IsOk())
        return status;

    return Status::OK();
}

Status Table::AddColumn(const std::string &name, ColumnType type,
                        size_t code_size) {
    ColumnId column_id;
    if (rep_->free_column_ids.size() > 0) {
        column_id = rep_->free_column_ids.front();
        rep_->free_column_ids.pop();
    } else {
        column_id = rep_->map.size();
    }
    Column *column = new Column(this, column_id, type, code_size);
    std::pair<std::map<std::string, Column *>::iterator, bool> ret;
    ret = rep_->map.insert(std::pair<std::string, Column *>(name, column));
    if (ret.second == false) {
        return Status::UsageError("The column name exists in the table.");
    }

    // Set the size of the new column
    Status status = column->Resize(rep_->num);
    if (!status.IsOk()) {
        return status;
    }
    return Status::OK();
}

Status Table::RemoveColumn(const std::string &name) {
    std::map<std::string, Column *>::iterator iter;
    iter = rep_->map.find(name);
    if (iter != rep_->map.end()) {
        // Add column id into free list
        rep_->free_column_ids.push(iter->second->GetColumnId());
        rep_->map.erase(iter);
    } else {
        return Status::UsageError("The column name doesn't exist.");
    }
    return Status::OK();
}

Column *Table::GetColumn(const std::string &name) const {
    std::map<std::string, Column *>::iterator iter;
    iter = rep_->map.find(name);
    if (iter != rep_->map.end()) {
        return iter->second;
    }
    return NULL;
}

ColumnId Table::GetMaxColumnId() const {
    ColumnId max_column_id = rep_->map.size() + rep_->free_column_ids.size();
    return max_column_id;
}

Column *Table::GetColumn(ColumnId column_id) const {
    if (column_id < GetMaxColumnId()) {
        std::map<std::string, Column *>::iterator iter;
        for (iter = rep_->map.begin(); iter != rep_->map.end(); ++iter) {
            if (iter->second->GetColumnId() == column_id)
                return iter->second;
        }
    }
    return NULL;
}

}  // namespace bitweaving

