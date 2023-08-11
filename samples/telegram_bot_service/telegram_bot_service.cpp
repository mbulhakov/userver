#include <userver/utest/using_namespace_userver.hpp>

#include <userver/clients/dns/component.hpp>
#include <userver/clients/http/component.hpp>
#include <userver/clients/http/form.hpp>
#include <userver/components/component.hpp>
#include <userver/components/loggable_component_base.hpp>
#include <userver/components/minimal_component_list.hpp>
#include <userver/concurrent/background_task_storage.hpp>
#include <userver/engine/wait_all_checked.hpp>
#include <userver/formats/json.hpp>
#include <userver/http/url.hpp>
#include <userver/logging/impl/logger_base.hpp>
#include <userver/logging/log.hpp>
#include <userver/testsuite/testsuite_support.hpp>
#include <userver/utils/async.hpp>
#include <userver/utils/daemon_run.hpp>
#include <userver/yaml_config/merge_schemas.hpp>

#include <boost/algorithm/string.hpp>
#include <boost/range/adaptors.hpp>
#include <boost/range/algorithm/max_element.hpp>
#include <fmt/format.h>

#include <optional>

namespace samples::telegram {

namespace {

constexpr auto kTelegramToken = "TG_BOT_TOKEN";
constexpr auto kTelegramUrlPrefix = "https://api.telegram.org/bot";
constexpr std::string_view kEchoCommand = "/echo ";

void log_bot_info(clients::http::Client &http_client, std::string_view token,
                  std::chrono::milliseconds network_timeout) {
  const auto get_me_url = fmt::format("{}{}/getMe", kTelegramUrlPrefix, token);

  const auto response = http_client.CreateRequest()
                            .get(get_me_url)
                            .timeout(network_timeout)
                            .perform();

  response->raise_for_status();

  const auto &bot_info =
      formats::json::FromString(response->body_view())["result"];
  const auto bot_name = bot_info["first_name"].As<std::string>();
  const auto bot_username = bot_info["username"].As<std::string>();

  LOG_INFO() << fmt::format("Starting worker for bot '{}' (@{})", bot_name,
                            bot_username);
}

void send_message(clients::http::Client &http_client, std::string token,
                  std::chrono::milliseconds network_timeout,
                  std::string chat_id, std::string message_id,
                  std::string text) {

  const auto send_message_url =
      fmt::format("{}{}/sendMessage", kTelegramUrlPrefix, token);

  clients::http::Form form;
  form.AddContent("chat_id", chat_id);
  form.AddContent("reply_to_message_id", message_id);
  form.AddContent("text", text);

  const auto response = http_client.CreateRequest()
                            .post(send_message_url, form)
                            .timeout(network_timeout)
                            .perform();

  response->raise_for_status();
}

void poll_updates(clients::http::Client &http_client, std::string token,
                  std::string task_processor_name,
                  std::chrono::milliseconds network_timeout,
                  std::chrono::seconds long_polling_timeout) {
  const auto get_updates_url =
      fmt::format("{}{}/getUpdates", kTelegramUrlPrefix, token);

  clients::http::Form form;
  form.AddContent("timeout", std::to_string(long_polling_timeout.count()));

  // Full list: [“message”, “edited_channel_post”, “callback_query”]
  form.AddContent("allowed_updates", "[\"message\"]");

  std::optional<int32_t> offset;
  while (!engine::current_task::ShouldCancel()) {
    try {
      if (offset) {
        form.AddContent("offset", std::to_string(*offset));
      }

      const auto response = http_client.CreateRequest()
                                .post(get_updates_url, form)
                                .timeout(long_polling_timeout)
                                .perform();

      response->raise_for_status();

      const auto &updates =
          formats::json::FromString(response->body_view())["result"];

      if (updates.IsEmpty()) {
        continue;
      }

      const auto latest_update_id =
          // without copy_range it won't work
          *boost::range::max_element(boost::copy_range<std::vector<int64_t>>(
              updates | boost::adaptors::transformed(
                            [](const formats::json::Value &update) {
                              return update["update_id"].As<int64_t>();
                            })));

      // https://core.telegram.org/bots/api#getupdates:
      // "Must be greater by one than the highest among the identifiers of
      // previously received updates."
      offset.emplace(latest_update_id + 1);

      std::vector<engine::Task> tasks;
      for (const auto &update : updates) {
        const auto &message = update["message"];

        auto text = message["text"].As<std::string>();
        if (boost::algorithm::starts_with(text, kEchoCommand) &&
            text != kEchoCommand) {

          std::string reply_text{text.data() + kEchoCommand.size(),
                                 text.size() - kEchoCommand.size()};

          auto message_id = std::to_string(message["message_id"].As<int64_t>());
          auto chat_id = std::to_string(message["chat"]["id"].As<int64_t>());

          tasks.push_back(utils::Async(
              task_processor_name, &send_message, std::ref(http_client), token,
              network_timeout, std::move(chat_id), std::move(message_id),
              std::move(reply_text)));
        }
        engine::WaitAllChecked(tasks);
      }
    } catch (const std::exception &e) {
      LOG_ERROR() << "Exception occured during updates' proccess: " << e.what();
    }
  }
}
} // namespace

class EchoBot final : public components::LoggableComponentBase {
public:
  // name of your component to refer in static config
  static constexpr std::string_view kName = "telegram-bot-service";

  EchoBot(const components::ComponentConfig &config,
          const components::ComponentContext &context);
  ~EchoBot() override final;

  static yaml_config::Schema GetStaticConfigSchema();

private:
  concurrent::BackgroundTaskStorage loop_;
};

EchoBot::EchoBot(const components::ComponentConfig &config,
                 const components::ComponentContext &context)
    : components::LoggableComponentBase(config, context) {
  const auto task_processor_name = config["task-processor"].As<std::string>();
  const auto long_polling_timeout =
      config["long-polling-timeout"].As<std::chrono::seconds>();
  const auto network_timeout =
      config["network-timeout"].As<std::chrono::milliseconds>();
  auto &http_client =
      context.FindComponent<components::HttpClient>().GetHttpClient();

  const auto token = std::getenv(kTelegramToken);

  loop_.AsyncDetach(task_processor_name, [token, long_polling_timeout,
                                          task_processor_name, network_timeout,
                                          &http_client] {
    try {
      if (logging::GetDefaultLogger().GetLevel() <= logging::Level::kInfo) {
        log_bot_info(http_client, token, network_timeout);
      }

      poll_updates(http_client, token, std::move(task_processor_name),
                   network_timeout, long_polling_timeout);
    } catch (const std::exception &e) {
      LOG_ERROR() << "Loop encountered exception: " << e.what();
      throw;
    }
  });
}

EchoBot::~EchoBot() { loop_.CancelAndWait(); }

yaml_config::Schema EchoBot::GetStaticConfigSchema() {
  return yaml_config::MergeSchemas<LoggableComponentBase>(R"(
type: object
description: Telegram bot worker
additionalProperties: false
properties:
    task-processor:
        type: string
        description: task processor to run HTTP requests
    long-polling-timeout:
        type: string
        description: timeout from https://core.telegram.org/bots/api#getupdates
        defaultDescription: 30s
    network-timeout:
        type: string
        description: timeout for regular HTTP requests
        defaultDescription: 1s
)");
}

} // namespace samples::telegram

int main(int argc, char *argv[]) {
  const auto component_list = components::MinimalComponentList()
                                  .Append<components::TestsuiteSupport>()
                                  .Append<clients::dns::Component>()
                                  .Append<components::HttpClient>()
                                  // custom components:
                                  .Append<samples::telegram::EchoBot>();
  return utils::DaemonMain(argc, argv, component_list);
}
