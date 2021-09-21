#pragma once

/// @file userver/utils/datetime/steady_coarse_clock.hpp
/// @brief @copybrief utils::datetime::SteadyCoarseClock

#include <chrono>

namespace utils::datetime {

/// @brief Steady clock with up to a few millisecond resulution that is slightly
/// faster than the std::chrono::steady_clock
struct SteadyCoarseClock {
  // Duration matches steady clock, but it is updated once in a few milliseconds
  using duration = std::chrono::steady_clock::duration;
  using rep = duration::rep;
  using period = duration::period;
  using time_point = std::chrono::time_point<SteadyCoarseClock, duration>;

  static constexpr bool is_steady = true;

  static time_point now() noexcept;
  static duration resolution() noexcept;
};

}  // namespace utils::datetime