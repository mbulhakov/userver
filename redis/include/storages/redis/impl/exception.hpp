#pragma once

/// @file redis/exception.hpp
/// @brief redis-specific exceptions

#include <stdexcept>

namespace redis {

/// Generic redis-related exception
class Exception : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

/// Invalid redis command argument
class InvalidArgumentException : public Exception {
 public:
  using Exception::Exception;
};

/// No reply from redis server
class RequestFailedException : public Exception {
 public:
  using Exception::Exception;
};

/// Request was cancelled
class RequestCancelledException : public Exception {
 public:
  using Exception::Exception;
};

/// Invalid reply data format
class ParseReplyException : public Exception {
 public:
  using Exception::Exception;
};

/// Invalid config format
class ParseConfigException : public Exception {
 public:
  using Exception::Exception;
};

}  // namespace redis