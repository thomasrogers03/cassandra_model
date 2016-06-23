require 'spec_helper'

module CassandraModel
  describe MetaTable do
    let(:connection_name) { nil }
    let(:table_name) { :records }
    let(:real_table_name) { definition.name_in_cassandra }

    let(:partition_key_types) { generate_partition_key_with_random_types }
    let(:partition_key) { partition_key_types.keys }
    let(:clustering_columns_types) { generate_clustering_columns_with_random_types }
    let(:clustering_columns) { clustering_columns_types.keys }
    let(:remaining_columns_types) { generate_fields_with_random_types }
    let(:remaining_columns) { remaining_columns_types.keys }
    let(:columns_types) { partition_key_types.merge(clustering_columns_types).merge(remaining_columns_types) }
    let(:columns) { columns_types.keys }

    let(:table_definition) do
      {name: table_name,
       partition_key: partition_key_types,
       clustering_columns: clustering_columns_types,
       remaining_columns: remaining_columns_types}
    end
    let(:definition) { TableDefinition.new(table_definition) }
    let(:klass) { MetaTable }
    let(:attributes) do
      {name: definition.name.to_s,
       created_at: Time.now,
       id: definition.table_id}
    end
    let(:table) { klass.new(connection_name, definition) }

    subject { table }

    before do
      TableDescriptor.create_descriptor_table
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
        before do
          allow(global_keyspace).to receive(:table).and_call_original
          allow(global_keyspace).to receive(:table).with(real_table_name) do
            allow(global_keyspace).to receive(:table).and_call_original
            nil
          end
        end

        context 'when the table does not yet exist' do
          describe 'creating the table' do
            let(:internal_table) { TableRedux.new(connection_name, real_table_name) }

            before { subject.public_send(method) }

            it { expect(internal_table.partition_key).to eq(partition_key) }
            it { expect(internal_table.clustering_columns).to eq(clustering_columns) }
            it { expect(internal_table.columns).to eq(columns) }
            it { expect(global_keyspace.table(real_table_name).columns.map(&:type)).to match_array(columns_types.values) }
          end
        end

        context 'when the table already exists in the descriptors table' do
          before { TableDescriptor.new(attributes).save }

          it 'should not create the table' do
            expect(global_session).not_to receive(:execute)
            subject.public_send(method) rescue nil
          end
        end

        context 'when the table already exists in the keyspace' do
          before do
            allow(global_keyspace).to receive(:table).and_call_original
            global_keyspace.add_table(real_table_name, [[partition_key], *clustering_columns], columns_types, true)
          end

          it 'should not attempt to create an entry into the descriptor table' do
            expect(global_session).not_to receive(:execute)
            subject.public_send(method) rescue nil
          end
        end

        context 'when creating the table raises an error' do
          let(:error) { StandardError.new('Could not create table!') }

          before { allow(global_session).to receive(:execute).and_raise(error) }

          it 'should remove the created TableDescriptor' do
            subject.public_send(method) rescue nil
            expect(TableDescriptor.first).to be_nil
          end

          it 'should re-raise the error' do
            expect { subject.public_send(method) }.to raise_error(error)
          end
        end

        describe 'consistency' do
          context 'when the table takes too long to create' do
            before { allow(global_keyspace).to receive(:table).with(real_table_name).and_return(nil) }

            it 'should raise an error' do
              expect { subject.public_send(method) }.to raise_error("Could not verify the creation of table #{definition.name_in_cassandra}")
            end

            it 'should delete the descriptor' do
              subject.public_send(method) rescue nil
              expect(TableDescriptor.first).to be_nil
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
