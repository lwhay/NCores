// This file copyright (c) 2011-2013, the BitWeaving authors.
// All rights reserved.
// See file LICENSE for details.

#include "bitweaving/status.h"

namespace bitweaving {

std::string Status::ToString() const {
    if (IsOk()) {
        return "OK";
    }
    switch (message_->code) {
        case kOk:
            return "OK";
        case kNotSupported:
            return "Feature not supported: " + message_->message;
        case kInvalidArgument:
            return "Invalid argument: " + message_->message;
        case kUsageError:
            return "Usage error: " + message_->message;
        case kIOError:
            return "IO error: " + message_->message;
    }
    return "Unknown error";
}

}  // namespace bitweaving
