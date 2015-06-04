module ConnectionHelper
  extend RSpec::Core::SharedContext

  MockColumn = Struct.new(:name, :type)

  let(:keyspace) { double(:keyspaceA, table: nil) }
  let(:query_results) { [] }
  let(:paginated_result) { MockPage.new(true, nil, query_results) }
  let(:default_statement) { double(:statement) }
  let(:connection) do
    double(:connection, execute_async: paginated_result, execute: paginated_result.get, prepare: default_statement)
  end
  let(:cluster) { double(:cassandra_cluster, connect: connection, keyspace: keyspace) }

  before do
    CassandraModel::ConnectionCache.reset!
    allow(Cassandra).to receive(:cluster).with(hash_including(hosts: %w(localhost))).and_return(cluster)
  end

  def mock_cluster(hosts)
    cluster = double(:cluster)
    allow(Cassandra).to receive(:cluster).with(hash_including(hosts: hosts)).and_return(cluster)
    cluster
  end

  def mock_connection(hosts, keyspace)
    cluster = mock_cluster(hosts)
    connection = double(:connection)
    allow(cluster).to receive(:connect).with(keyspace.to_s).and_return(connection)
    connection
  end

  def mock_prepare(query)
    statement = double(:statement)
    allow(connection).to receive(:prepare).with(query).and_return(statement)
    statement
  end

  def mock_query_pages(results)
    result_future = MockPage.new(true, nil, results.shift || [])
    while (current_result = results.shift)
      result_future = MockPage.new(false, result_future, current_result)
    end
    result_future
  end

  def mock_query_result(args, results = [])
    result_future = mock_query_pages(results)
    allow(connection).to receive(:execute_async).with(*args).and_return(result_future)
  end

  def mock_table(name, partition_key, clustering_columns, remaining_columns)
    mock_table_for_keyspace(keyspace, name, partition_key, clustering_columns, remaining_columns)
  end

  def mock_table_for_keyspace(keyspace, name, partition_key, clustering_columns, remaining_columns)
    table_pk = partition_key.map { |name, type| MockColumn.new(name.to_s, type) }
    table_ck = clustering_columns.map { |name, type| MockColumn.new(name.to_s, type) }
    table_columns = (partition_key.merge(clustering_columns.merge(remaining_columns))).map do |name, type|
      MockColumn.new(name.to_s, type)
    end
    table = double(:table, partition_key: table_pk, clustering_columns: table_ck, columns: table_columns)
    allow(keyspace).to receive(:table).with(name.to_s).and_return(table)
  end

  def mock_simple_table(name, partition_columns, clustering_columns, column_names)
    mock_simple_table_for_keyspace(keyspace, name, partition_columns, clustering_columns, column_names)
  end

  def mock_simple_table_for_keyspace(keyspace, name, partition_columns, clustering_columns, column_names)
    partition_key = partition_columns.inject({}) { |memo, column| memo.merge!(column => :text) }
    clustering_columns = clustering_columns.inject({}) { |memo, column| memo.merge!(column => :text) }
    remaining_columns = column_names.inject({}) { |memo, column| memo.merge!(column => :text) }
    mock_table_for_keyspace(keyspace, name, partition_key, clustering_columns, remaining_columns)
  end
end