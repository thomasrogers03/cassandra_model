require 'rspec'

module CassandraModel
  describe MetaTable do
    let(:table_name) { :records }
    let(:table_definition) do
      {name: table_name,
       partition_key: {partition_key: :text},
       clustering_columns: {cluster: :text},
       remaining_columns: {meta_data: 'map<text, text>'}}
    end
    let(:definition) { TableDefinition.new(table_definition) }
    let(:cluster) { double(:cluster, connect: connection) }
    let(:connection) { double(:connection, execute: []) }
    let(:column_object) { double(:column, name: 'partition') }
    let(:table_object) { double(:table, columns: [column_object], partition_key: [], clustering_columns: []) }
    let(:keyspace) { double(:keyspace, table: table_object) }
    let(:klass) { MetaTable }
    let(:attributes) do
      {name: definition.name.to_s,
       created_at: Time.now,
       id: definition.table_id}
    end
    let(:valid) { true }
    let(:descriptor) do
      TableDescriptor.new(attributes).tap { |desc| desc.invalidate! unless valid }
    end

    subject { klass.new(definition) }

    before do
      klass.reset!
      allow(Cassandra).to receive(:cluster).and_return(cluster)
      allow(cluster).to receive(:keyspace).with(klass.config[:keyspace]).and_return(keyspace)
      TableDescriptor.reset!
      TableDescriptor.columns = [:name, :created_at, :id]
      allow(TableDescriptor).to receive(:create).with(definition).and_return(descriptor)
    end

    it_behaves_like 'a model with a connection', MetaTable
    it_behaves_like 'a table'

    [:partition_key, :clustering_columns, :columns].each do |method|
      describe "#{method}" do

        context 'when the table does not yet exist' do
          it 'should create the table' do
            expect(connection).to receive(:execute).with(definition.to_cql)
            subject.public_send(method)
          end
        end

        context 'when the table already exists' do
          let(:valid) { false }

          it 'should create the table' do
            expect(connection).not_to receive(:execute).with(definition.to_cql)
            subject.public_send(method)
          end
        end
      end
    end

  end
end