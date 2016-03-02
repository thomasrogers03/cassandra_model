require 'rspec'

module CassandraModel
  describe MetaTable do
    TABLE_POSTFIX = '_50306970412fc32e13cfe807ba6426de'

    let(:connection_name) { nil }
    let(:table_name) { :records }
    let(:real_table_name) { "#{table_name}#{TABLE_POSTFIX}".to_sym }
    let(:table_definition) do
      {name: table_name,
       partition_key: {partition_key: :text},
       clustering_columns: {cluster: :text},
       remaining_columns: {meta_data: 'map<text, text>'}}
    end
    let(:definition) { TableDefinition.new(table_definition) }
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
    let(:table) { klass.new(connection_name, definition) }

    subject { table }

    before do
      mock_simple_table(:table_descriptors, [:name], [:created_at], [:id])
      mock_simple_table(real_table_name, [:partition], [], [])
      allow(TableDescriptor).to receive(:create).with(definition).and_return(descriptor)
      allow_any_instance_of(MetaTable).to receive(:sleep)
    end

    it { is_expected.to be_a_kind_of(TableRedux) }

    describe '#==' do
      it 'should should be equal when the connections and table definitions are the same' do
        expect(table).to eq(klass.new(connection_name, definition))
      end

      context 'when the connection names are different' do
        it 'should be equal' do
          expect(table).not_to eq(klass.new(:single, definition))
        end
      end

      context 'when the table definitions are different' do
        let(:other_table_definition) do
          {name: :images,
           partition_key: {author: :text},
           clustering_columns: {title: :text},
           remaining_columns: {price: :double}}
        end
        let(:other_definition) { TableDefinition.new(other_table_definition) }

        it 'should not be equal' do
          expect(table).not_to eq(klass.new(connection_name, other_definition))
        end
      end
    end

    describe '#connection' do
      it 'should be the cached cassandra connection' do
        expect(subject.connection).to eq(ConnectionCache[nil])
      end

      context 'with the connection name parameter omitted' do
        let(:table) { MetaTable.new(table_name) }

        it 'should be the cached cassandra connection' do
          expect(subject.connection).to eq(ConnectionCache[nil])
        end
      end

      context 'with a different connection name' do
        let(:connection_name) { :counters }
        let(:hosts) { %w(cassandra.one cassandra.two) }
        let!(:connection) { mock_connection(hosts, 'keyspace') }

        before { ConnectionCache[:counters].config = {hosts: hosts, keyspace: 'keyspace'} }

        it 'should use the specified connection' do
          expect(subject.connection).to eq(ConnectionCache[:counters])
        end
      end
    end

    describe '#reset_local_schema!' do
      it 'should indicate that this functionality is not implemented' do
        expect { subject.reset_local_schema! }.to raise_error(Cassandra::Errors::ClientError, 'Schema changes are not supported for meta tables')
      end
    end

    describe '#name' do
      subject { klass.new(connection_name, definition).name }

      it 'should be the generated name from the table definition' do
        is_expected.to eq(definition.name_in_cassandra)
      end
    end

    shared_examples_for 'a method requiring the table to exist' do |method|
      describe "#{method}" do
        let(:cql) { definition.to_cql(check_exists: true) }
        let(:bad_keyspace) { double(:keyspace, table: nil) }

        before do
          allow(cluster).to receive(:keyspace) do
            allow(cluster).to receive(:keyspace).and_return(keyspace)
            bad_keyspace
          end
        end

        context 'when the table does not yet exist' do
          it 'should create the table' do
            expect(connection).to receive(:execute).with(cql)
            subject.public_send(method)
          end
        end

        context 'when the table already exists in the descriptors table' do
          let(:valid) { false }

          it 'should not create the table' do
            expect(connection).not_to receive(:execute).with(cql)
            subject.public_send(method)
          end
        end

        context 'when the table already exists in the keyspace' do
          before { allow(cluster).to receive(:keyspace).and_return(keyspace) }

          it 'should not attempt to create an entry into the descriptor table' do
            expect(connection).not_to receive(:execute)
            subject.public_send(method)
          end
        end

        context 'when creating the table raises an error' do
          let(:error) { StandardError.new('Could not create table!') }

          before { allow(connection).to receive(:execute).and_raise(error) }

          it 'should remove the created TableDescriptor' do
            expect(descriptor).to receive(:delete)
            subject.public_send(method) rescue nil
          end

          it 'should re-raise the error' do
            expect { subject.public_send(method) }.to raise_error(error)
          end
        end

        describe 'consistency' do
          it 'should wait until the schema says the table exists' do
            allow(cluster).to receive(:keyspace).and_return(bad_keyspace, bad_keyspace, keyspace)
            expect(subject.columns).to eq([:partition])
          end

          context 'when the table takes too long to create' do
            before do
              allow(cluster).to receive(:keyspace).and_return(bad_keyspace)
              allow(descriptor).to receive(:delete)
            end

            it 'should raise an error' do
              expect { subject.columns }.to raise_error("Could not verify the creation of table #{definition.name_in_cassandra}")
            end

            it 'should raise an error' do
              expect(descriptor).to receive(:delete)
              expect { subject.columns }.to raise_error("Could not verify the creation of table #{definition.name_in_cassandra}")
            end
          end

        end
      end
    end

    it_behaves_like 'a method requiring the table to exist', :name
    it_behaves_like 'a method requiring the table to exist', :partition_key
    it_behaves_like 'a method requiring the table to exist', :clustering_columns
    it_behaves_like 'a method requiring the table to exist', :columns

  end
end
