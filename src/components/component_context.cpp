#include <components/component_context.hpp>

#include <queue>

#include <boost/algorithm/string/join.hpp>

#include <engine/async.hpp>
#include <engine/task/task_processor.hpp>
#include <logging/log.hpp>
#include <tracing/tracer.hpp>

namespace components {

namespace {
const std::string kStopComponentRootName = "all_components_stop";
const std::string kStoppingComponent = "component_stopping";
const std::string kStopComponentName = "component_stop";
const std::string kComponentName = "component_name";
}  // namespace

ComponentContext::ComponentContext(const Manager& manager,
                                   TaskProcessorMap task_processor_map)
    : manager_(manager), task_processor_map_(std::move(task_processor_map)) {}

void ComponentContext::BeforeAddComponent(std::string name) {
  std::lock_guard<engine::Mutex> lock(component_mutex_);
  task_to_component_map_[engine::current_task::GetCurrentTaskContext()] =
      std::move(name);
}

void ComponentContext::AddComponent(
    std::string name, std::unique_ptr<ComponentBase>&& component) {
  std::lock_guard<engine::Mutex> lock(component_mutex_);

  components_.emplace(name, std::move(component));
  component_names_.push_back(std::move(name));

  component_cv_.NotifyAll();
}

void ComponentContext::ClearComponents() {
  auto root_span = tracing::Tracer::GetTracer()->CreateSpanWithoutParent(
      kStopComponentRootName);
  TRACE_INFO(root_span) << "Sending stopping notification to all components";
  OnAllComponentsAreStopping(root_span);
  TRACE_INFO(root_span) << "Stopping components";

  std::vector<engine::TaskWithResult<void>> unload_tasks;
  {
    std::lock_guard<engine::Mutex> lock(component_mutex_);
    for (const auto& name : component_names_)
      unload_tasks.emplace_back(engine::Async([&root_span, this, name]() {
        WaitAndUnloadComponent(root_span, name);
      }));
  }

  for (auto& task : unload_tasks) task.Get();

  TRACE_INFO(root_span) << "Stopped all components";
}

void ComponentContext::OnAllComponentsAreStopping(tracing::Span& parent_span) {
  std::lock_guard<engine::Mutex> lock(component_mutex_);

  for (auto& component_item : components_) {
    try {
      auto span = parent_span.CreateChild(kStoppingComponent);
      component_item.second->OnAllComponentsAreStopping();
    } catch (const std::exception& e) {
      const auto& name = component_item.first;
      TRACE_ERROR(parent_span)
          << "Exception while sendind stop notification to component " + name +
                 ": " + e.what();
    }
  }
}

void ComponentContext::OnAllComponentsLoaded() {
  std::lock_guard<engine::Mutex> lock(component_mutex_);
  for (auto& component_item : components_) {
    component_item.second->OnAllComponentsLoaded();
  }
}

size_t ComponentContext::ComponentCount() const {
  std::lock_guard<engine::Mutex> lock(component_mutex_);
  return components_.size();
}

const Manager& ComponentContext::GetManager() const { return manager_; }

ComponentBase* ComponentContext::DoFindComponentNoWait(
    const std::string& name, std::unique_lock<engine::Mutex>&) const {
  const auto it = components_.find(name);
  if (it != components_.cend()) {
    return it->second.get();
  }
  return nullptr;
}

void ComponentContext::CheckForDependencyLoop(
    const std::string& entry_name, std::unique_lock<engine::Mutex>&) const {
  std::queue<std::string> todo;
  std::set<std::string> handled;
  std::unordered_map<std::string, std::string> parent;

  todo.push(entry_name);
  handled.insert(entry_name);
  parent[entry_name] = std::string();

  while (!todo.empty()) {
    const auto cur_name = std::move(todo.front());
    todo.pop();

    if (component_dependencies_.count(cur_name) > 0) {
      for (const auto& name : component_dependencies_[cur_name]) {
        if (name == entry_name) {
          std::vector<std::string> dependency_chain;
          dependency_chain.push_back(name);
          for (auto it = cur_name; !it.empty(); it = parent[it])
            dependency_chain.push_back(it);

          LOG_ERROR() << "Found circular dependency between components: "
                      << boost::algorithm::join(dependency_chain, " -> ");
          throw std::runtime_error("circular dependency");
        }

        if (handled.count(name) > 0) continue;

        todo.push(name);
        handled.insert(name);
        parent.emplace(name, cur_name);
      }
    }
  }
}

void ComponentContext::CancelComponentsLoad() {
  std::lock_guard<engine::Mutex> lock(component_mutex_);
  components_load_cancelled_ = true;

  component_cv_.NotifyAll();
}

std::string ComponentContext::GetLoadingComponentName(
    std::unique_lock<engine::Mutex>&) const {
  try {
    return task_to_component_map_.at(
        engine::current_task::GetCurrentTaskContext());
  } catch (const std::exception&) {
    throw std::runtime_error(
        "FindComponent() can be called only from a task of component load");
  }
}

void ComponentContext::AddDependency(const std::string& name) const {
  std::unique_lock<engine::Mutex> lock(component_mutex_);

  const auto& current_component = GetLoadingComponentName(lock);

  auto& current_component_dependency =
      component_dependencies_[current_component];

  if (current_component_dependency.count(name) == 0) {
    LOG_INFO() << "Resolving dependency " << current_component << " -> "
               << name;

    current_component_dependency.insert(name);
    CheckForDependencyLoop(name, lock);
  }
}

void ComponentContext::RemoveComponentDependencies(const std::string& name) {
  LOG_INFO() << "Removing " << name << " from component dependees";
  std::lock_guard<engine::Mutex> lock(component_mutex_);
  component_dependencies_.erase(name);

  component_cv_.NotifyAll();
}

ComponentBase* ComponentContext::DoFindComponent(
    const std::string& name) const {
  AddDependency(name);

  std::unique_lock<engine::Mutex> lock(component_mutex_);

  if (loading_component_names_.count(name) == 0)
    throw std::runtime_error("Requested non-existing component " + name);

  auto component = DoFindComponentNoWait(name, lock);
  if (component) return component;

  LOG_INFO() << "component " << name << " is not loaded yet, component "
             << GetLoadingComponentName(lock) << " is waiting for it to load";

  component_cv_.Wait(lock, [this, &lock, &component, &name]() {
    if (components_load_cancelled_) return true;
    component = DoFindComponentNoWait(name, lock);
    return component != nullptr;
  });

  if (components_load_cancelled_)
    throw std::runtime_error("Components load cancelled");
  return component;
}

engine::TaskProcessor* ComponentContext::GetTaskProcessor(
    const std::string& name) const {
  auto it = task_processor_map_.find(name);
  if (it == task_processor_map_.cend()) {
    return nullptr;
  }
  return it->second.get();
}

ComponentContext::TaskProcessorPtrMap ComponentContext::GetTaskProcessorsMap()
    const {
  TaskProcessorPtrMap result;
  for (const auto& it : task_processor_map_)
    result.emplace(it.first, it.second.get());

  return result;
}

void ComponentContext::SetLoadingComponentNames(std::set<std::string> names) {
  std::unique_lock<engine::Mutex> lock(component_mutex_);
  loading_component_names_ = std::move(names);
}

bool ComponentContext::MayUnload(const std::string& name) const {
  // If there is any component that depends on 'name' then no
  for (const auto& it : component_dependencies_) {
    const auto& deps = it.second;
    if (deps.count(name) > 0) {
      LOG_INFO() << "Component " << name << " may not unload yet (" << it.first
                 << " depends on " << name << ")";
      return false;
    }
  }

  LOG_INFO() << "Component " << name << " may unload now";
  return true;
}

void ComponentContext::WaitAndUnloadComponent(tracing::Span& root_span,
                                              const std::string& name) {
  std::unique_ptr<ComponentBase> tmp;

  TRACE_INFO(root_span) << "Preparing to stop component " << name;

  {
    std::unique_lock<engine::Mutex> lock(component_mutex_);
    component_cv_.Wait(lock, [this, &name]() { return MayUnload(name); });

    std::swap(tmp, components_[name]);
    components_.erase(name);
  }

  // Actual stop without the mutex
  {
    auto span = root_span.CreateChild(kStopComponentName);
    span.AddTag(kComponentName, name);

    TRACE_INFO(span) << "Stopping component";
    tmp.reset();
    TRACE_INFO(span) << "Stopped component";
  }

  RemoveComponentDependencies(name);
}

}  // namespace components
