module ConnectionHelper
  extend RSpec::Core::SharedContext

  MockColumn = Struct.new(:name, :type)

  class MockBoundStatement
    attr_reader :query, :args

    def initialize(query, args)
      @query = query
      @args = args
    end

    def inspect
      "<Prepared Statement::{#{query}}[#{args.map(&:inspect) * ','}]>"
    end
  end

  class MockStatement
    attr_reader :query

    def initialize(query)
      @query = query
    end

    def bind(*args)
      MockBoundStatement.new(query, args)
    end

    def inspect
      "<Prepared Statement::{#{query}}>"
    end
  end

  class DummyStatement < MockStatement
    def inspect
      "<Prepared Dummy Statement::{#{query}}>"
    end
  end

  let(:connection_name) { nil }
  let(:keyspace_name) { 'default_keyspace' }
  let(:query_results) { [] }
  let(:paginated_result) { MockPage.new(true, nil, query_results) }
  let(:paginated_result_future) { MockFuture.new(paginated_result) }
  let(:global_cluster) { CassandraModel::ConnectionCache[connection_name].cluster }
  let(:global_keyspace) { CassandraModel::ConnectionCache[connection_name].keyspace }
  let(:global_session) { CassandraModel::ConnectionCache[connection_name].session }

  before do
    CassandraModel::ConnectionCache.reset!
  end

  def mock_prepare(query)
    CassandraModel::ConnectionCache[connection_name].statement(query) if query
  end

  def mock_table(name, partition_key, clustering_columns, remaining_columns)
    mock_table_for_keyspace(keyspace_name, name, partition_key, clustering_columns, remaining_columns)
  end

  def mock_table_for_keyspace(keyspace, name, partition_key, clustering_columns, remaining_columns)
    raise 'Invalid keyspace' unless keyspace.is_a?(String)
    CassandraModel::ConnectionCache[connection_name].keyspace.add_table(
        name.to_s,
        [partition_key.keys.map(&:to_s), *clustering_columns.keys.map(&:to_s)],
        partition_key.merge(clustering_columns).merge(remaining_columns).stringify_keys,
        false
    )
  end

  def mock_simple_table(name, partition_columns, clustering_columns, column_names)
    mock_simple_table_for_keyspace(keyspace_name, name, partition_columns, clustering_columns, column_names)
  end

  def mock_simple_table_for_keyspace(keyspace, name, partition_columns, clustering_columns, column_names)
    partition_key = partition_columns.inject({}) { |memo, column| memo.merge!(column => :text) }
    clustering_columns = clustering_columns.inject({}) { |memo, column| memo.merge!(column => :text) }
    remaining_columns = column_names.inject({}) { |memo, column| memo.merge!(column => :text) }
    mock_table_for_keyspace(keyspace, name, partition_key, clustering_columns, remaining_columns)
  end
end
