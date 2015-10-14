module BatchHelper
  extend RSpec::Core::SharedContext

  let(:global_reactor) { double(:reactor) }

  def mock_reactor(cluster, type, options)
    allow(CassandraModel::BatchReactor).to receive(:new).with(cluster, cluster.connect, type, options).and_return(global_reactor)
  end
end
