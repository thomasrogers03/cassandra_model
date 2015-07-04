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

  end
end