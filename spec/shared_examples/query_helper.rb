module CassandraModel
  shared_examples_for 'a query helper' do
    shared_examples_for 'a query helper method' do |method, args|
      let(:query_builder) { double(:query_builder) }

      before do
        allow(QueryBuilder).to receive(:new).with(subject).and_return(query_builder)
      end

      it "should delegate #{method} to new QueryBuilder" do
        expect(query_builder).to receive(method).with(args)
        subject.send(method, args)
      end
    end

    it_behaves_like 'a query helper method', :where, { partition: 'Partition Key' }
    it_behaves_like 'a query helper method', :select, :partition
    it_behaves_like 'a query helper method', :paginate, 5000
    it_behaves_like 'a query helper method', :order, :cluster
    it_behaves_like 'a query helper method', :limit, 100
  end
end