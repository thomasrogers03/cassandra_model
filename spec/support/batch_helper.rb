module BatchHelper
  extend RSpec::Core::SharedContext

  let(:reactor_started_future) { double(:future, get: nil) }
  let(:global_reactor) { double(:reactor, started_future: reactor_started_future, start: reactor_started_future) }

  def mock_reactor(cluster, session, type, options)
    allow(CassandraModel::BatchReactor).to receive(:new).with(cluster, session, type, options).and_return(global_reactor)
  end
end
