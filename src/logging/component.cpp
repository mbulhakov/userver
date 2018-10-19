#include <logging/component.hpp>

#include <chrono>
#include <stdexcept>

// this header must be included before any spdlog headers
// to override spdlog's level names
#include <logging/spdlog.hpp>

#include <spdlog/async.h>

#include <logging/log.hpp>
#include <logging/logger.hpp>
#include <logging/reopening_file_sink.hpp>
#include <yaml_config/value.hpp>

#include "config.hpp"

namespace components {
namespace {

std::chrono::seconds kDefaultFlushInterval{2};

}  // namespace

Logging::Logging(const ComponentConfig& config, const ComponentContext&) {
  auto loggers = config.Yaml()["loggers"];
  auto loggers_full_path = config.FullPath() + ".loggers";
  yaml_config::CheckIsMap(loggers, loggers_full_path);

  for (auto it = loggers.begin(); it != loggers.end(); ++it) {
    auto logger_name = it->first.as<std::string>();
    auto logger_full_path = loggers_full_path + '.' + logger_name;
    bool is_default_logger = logger_name == "default";

    auto logger_config = logging::LoggerConfig::ParseFromYaml(
        it->second, logger_full_path, config.ConfigVarsPtr());

    auto overflow_policy = spdlog::async_overflow_policy::overrun_oldest;
    if (logger_config.queue_overflow_behavior ==
        logging::LoggerConfig::QueueOveflowBehavior::kBlock) {
      overflow_policy = spdlog::async_overflow_policy::block;
    }

    auto file_sink =
        std::make_shared<logging::ReopeningFileSinkMT>(logger_config.file_path);
    std::shared_ptr<spdlog::details::thread_pool> tp;
    if (is_default_logger) {
      spdlog::init_thread_pool(logger_config.message_queue_size,
                               logger_config.thread_pool_size);
      tp = spdlog::thread_pool();
    } else {
      tp = std::make_shared<spdlog::details::thread_pool>(
          logger_config.message_queue_size, logger_config.thread_pool_size);
      thread_pools_.push_back(tp);
    }
    auto logger = std::make_shared<spdlog::async_logger>(
        logger_name, std::move(file_sink), tp, overflow_policy);
    logger->set_level(
        static_cast<spdlog::level::level_enum>(logger_config.level));
    logger->set_pattern(logger_config.pattern);
    logger->flush_on(
        static_cast<spdlog::level::level_enum>(logger_config.flush_level));

    if (is_default_logger) {
      auto old_default_logger = logging::SetDefaultLogger(std::move(logger));
      if (old_default_logger) {
        // Close file sinks
        for (auto s : old_default_logger->sinks()) {
          auto reop =
              std::dynamic_pointer_cast<logging::ReopeningFileSinkMT>(s);
          if (reop) {
            reop->Close();
          }
        }
      }
    } else {
      auto insertion_result =
          loggers_.emplace(std::move(logger_name), std::move(logger));
      if (!insertion_result.second) {
        throw std::runtime_error("duplicate logger '" +
                                 insertion_result.first->first + '\'');
      }
    }
  }
  flush_task_.Start("log_flusher",
                    utils::PeriodicTask::Settings(
                        std::chrono::duration_cast<std::chrono::milliseconds>(
                            kDefaultFlushInterval)),
                    GetTaskFunction());
}

Logging::~Logging() { flush_task_.Stop(); }

logging::LoggerPtr Logging::GetLogger(const std::string& name) {
  auto it = loggers_.find(name);
  if (it == loggers_.end()) {
    throw std::runtime_error("logger '" + name + "' not found");
  }
  return it->second;
}

void Logging::OnLogRotate() {
  auto reopen_all = [](auto& sinks) {
    for (auto s : sinks) {
      auto reop = std::dynamic_pointer_cast<logging::ReopeningFileSinkMT>(s);
      if (reop) {
        // TODO Handle exceptions here
        reop->Reopen(/* truncate = */ false);
      }
    }
  };

  // this must be a copy
  auto default_logger = logging::DefaultLogger();
  reopen_all(default_logger->sinks());

  for (const auto& item : loggers_) {
    reopen_all(item.second->sinks());
  }
}

void Logging::FlushLogs(tracing::Span&&) {
  logging::DefaultLogger()->flush();
  for (auto& item : loggers_) {
    item.second->flush();
  }
}

}  // namespace components
