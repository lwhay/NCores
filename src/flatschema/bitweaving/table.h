// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#ifndef BITWEAVING_INCLUDE_TABLE_H_
#define BITWEAVING_INCLUDE_TABLE_H_

#include <string>

#include "bitweaving/column.h"
#include "bitweaving/iterator.h"
#include "bitweaving/options.h"
#include "bitweaving/status.h"
#include "bitweaving/types.h"

namespace bitweaving {

class Column;

class Iterator;

/**
 * @brief Class for storing and accessing data in a table, which contains a
 * set of Column.
 */
class Table {
public:
    /**
     * @brief Constructor.
     * @param path The location of its associated data file.
     * @param options The options control the table.
     */
    Table(const std::string &path, const Options &options);

    /**
     * @brief Destructor.
     */
    ~Table();

    /**
     * @brief Get the number of tuples in this table.
     * @return The number of tuples in this table.
     */
    size_t GetNumTuples() const;

    /**
     * @brief Set the number of tuples in this table.
     * @param num_tuples The number of tuples to set.
     * @return A Status object to indicate success or failure.
     */
    Status Resize(size_t num_tuples);

    /**
     * @brief Open the table. Load data if the file exists.
     * @return A Status object to indicate success or failure.
     */
    Status Open();

    /**
     * @brief Close and save the table.
     * @return A Status object to indicate success or failure.
     */
    Status Close();

    /**
     * @brief Read the table from the file.
     * @return A Status object to indicate success or failure.
     */
    Status Load();

    /**
     * @brief Write the table to the file.
     * @return A Status object to indicate success or failure.
     */
    Status Save();

    /**
     * @brief Delete the directory for this table.
     * @return A Status object to indicate success or failure.
     */
    Status Delete();

    /**
     * @brief Add a new column into this table.
     * @param name The column name.
     * @param type Type type of the column (Naive/BitWeavingH/BitWeavingV).
     * @param code_size The number of bits to represent a code in this column
     * @return A Status object to indicate success or failure.
     */
    Status AddColumn(const std::string &name, ColumnType type, size_t code_size);

    /**
     * @brief Remove an existed column from this table.
     * @param name The column name.
     * @return A Status object to indicate success or failure.
     */
    Status RemoveColumn(const std::string &name);

    /**
     * @brief Get an attribute by its column name.
     * @param name The column name to search for.
     * @return The pointer to the attribute with the given name.
     **/
    Column *GetColumn(const std::string &name) const;

private:
    /**
     * @brief Load meta data file.
     * @param filename The name of meta data file.
     * @return A Status object to indicate success or failure.
     */
    Status LoadMetaFile(const std::string &filename);

    /**
     * @brief Save meta data file.
     * @param filename The name of meta data file.
     * @return A Status object to indicate success or failure.
     */
    Status SaveMetaFile(const std::string &filename);

    /**
     * @brief Get the maximum column ID.
     * @return The maximum column ID.
     */
    ColumnId GetMaxColumnId() const;

    /**
     * @brief Get an attribute by its column ID.
     * @param name The column ID to search for.
     * @return The pointer to the attribute with the given ID.
     **/
    Column *GetColumn(ColumnId column_id) const;

    struct Rep;
    Rep *rep_;

    friend class Iterator;
};

}  // namespace bitweaving

#endif  // BITWEAVING_INCLUDE_TABLE_H_
