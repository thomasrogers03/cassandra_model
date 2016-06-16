module PerformanceHelper
  extend RSpec::Core::SharedContext

  let(:performance_logger) { ThomasUtils::InMemoryLogger.new }
  let(:performance_monitor) { ThomasUtils::PerformanceMonitor.new(performance_logger) }

  before do
    ThomasUtils::PerformanceMonitorMixin.monitor = performance_monitor
  end
end
