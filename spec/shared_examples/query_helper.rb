module CassandraModel
  shared_examples_for 'a query helper' do
    let(:query_builder) { double(:query_builder) }

    before do
      allow(QueryBuilder).to receive(:new).with(subject).and_return(query_builder)
    end

    shared_examples_for 'a query helper method' do |method, args|
      it "should delegate #{method} to new QueryBuilder" do
        args = [no_args] if args.empty?
        expect(query_builder).to receive(method).with(*args)
        subject.send(method, *args)
      end
    end

    it_behaves_like 'a query helper method', :where, [{partition: 'Partition Key'}]
    it_behaves_like 'a query helper method', :select, [:partition, :clustering]
    it_behaves_like 'a query helper method', :pluck, [:partition, :clustering]
    it_behaves_like 'a query helper method', :paginate, [5000]
    it_behaves_like 'a query helper method', :each_slice, [1000]
    it_behaves_like 'a query helper method', :order, [:cluster]
    it_behaves_like 'a query helper method', :limit, [100]

    describe '#find_by' do
      let(:attributes) { {partition: 'Partition Key'} }

      it 'should return the first item from QueryBuilder#where' do
        expect(query_builder).to receive(:where).with(attributes).and_return(query_builder)
        expect(query_builder).to receive(:first)
        subject.find_by(attributes)
      end

      context 'with different attributes' do
        let(:attributes) { {partition: 'Partition Key', clustering_column: 'Cluster'} }

        it 'should return the first item from QueryBuilder#where' do
          expect(query_builder).to receive(:where).with(attributes).and_return(query_builder)
          expect(query_builder).to receive(:first)
          subject.find_by(attributes)
        end
      end
    end

    describe '#all' do
      it 'should delegate to QueryBuilder#where with empty attributes' do
        expect(query_builder).to receive(:where).with({})
        subject.all
      end
    end

    shared_examples_for 'a cluster paginating query method' do |method, operator|
      describe "##{method}" do
        let(:record_partition_key) { {part: 'Partition Key'} }
        let(:record_clustering_columns) { {cluster1: 'Cluster This', cluster2: 'Cluster That'} }
        let(:cluster_comparer) { {record_clustering_columns.keys.public_send(operator) => record_clustering_columns.values} }
        let(:record) { double(:record, partition_key: record_partition_key, clustering_columns: record_clustering_columns) }

        it 'should query for the records whose partition key is the same and clustering columns are greater than the current ones' do
          expect(query_builder).to receive(:where).with(record_partition_key.merge(cluster_comparer))
          subject.public_send(method, record)
        end
      end
    end

    it_behaves_like 'a cluster paginating query method', :after, :gt
    it_behaves_like 'a cluster paginating query method', :before, :lt

  end
end
