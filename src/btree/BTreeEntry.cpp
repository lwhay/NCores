#include "BTreeEntry.h"
#include <string.h>

int BTreeLEntry::get_size() {
    return sizeof(KEY_TYPE) + sizeof(int) + record.get_size();
}

void BTreeLEntry::read_from_buffer(char *buffer) {
    memcpy(&key, buffer, sizeof(KEY_TYPE));
    int i = sizeof(KEY_TYPE);
    memcpy(&rid, &(buffer[i]), sizeof(int));
    i += sizeof(int);
    record.read_from_buffer(&(buffer[i]));
}

void BTreeLEntry::write_to_buffer(char *buffer) {
    memcpy(buffer, &key, sizeof(KEY_TYPE));
    int i = sizeof(KEY_TYPE);
    memcpy(&(buffer[i]), &rid, sizeof(int));
    i += sizeof(int);
    record.write_to_buffer(&(buffer[i]));
}

BTreeLEntry &BTreeLEntry::operator=(BTreeLEntry &_d) {
    key = _d.key;
    rid = _d.rid;
    record = _d.record;
    return *this;
}
