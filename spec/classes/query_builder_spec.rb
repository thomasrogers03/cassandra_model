require 'rspec'

module CassandraModel
  describe QueryBuilder do
    class MockQueryResult
      attr_reader :attributes

      def initialize(attributes)
        @attributes = attributes
      end
    end

    let(:results) { %w(results) }
    let(:page_result) { MockPage.new(true, nil, results) }
    let(:page_result_future) { MockFuture.new(page_result) }
    let(:result_paginator) { ResultPaginator.new(page_result_future) { |row| row } }
    let(:record) { double(:record, request_async: result_paginator, request: results, request_cql: nil) }

    subject { QueryBuilder.new(record) }

    it { is_expected.to be_a_kind_of(Enumerable) }

    shared_examples_for 'a method returning the builder' do |method|
      it 'should return itself' do
        expect(subject.send(method, params)).to eq(subject)
      end
    end

    describe '#async' do
      it 'should execute the built query asynchronously' do
        expect(record).to receive(:request_async).with({}, {})
        subject.async
      end
    end

    describe '#get' do
      it 'should execute the built query' do
        expect(record).to receive(:request).with({}, {})
        subject.get
      end
    end

    describe '#first_async' do
      it 'should execute the built query' do
        expect(record).to receive(:first_async).with({}, {})
        subject.first_async
      end
    end

    describe '#first' do
      it 'should execute the built query' do
        expect(record).to receive(:first).with({}, {})
        subject.first
      end
    end

    describe '#pluck' do
      let(:first_result) { MockQueryResult.new(column1: 'hello', column2: 'world', column3: 'good bye!') }
      let(:results) { [first_result] }
      let(:pluck_columns) { [:column1] }

      subject { QueryBuilder.new(record).pluck(*pluck_columns) }

      it 'should grab the columns from the resulting records' do
        is_expected.to eq([%w(hello)])
      end

      context 'with different pluck columns' do
        let(:pluck_columns) { [:column2, :column3] }

        it { is_expected.to eq([['world', 'good bye!']]) }
      end

      context 'with multiple results' do
        let(:second_result) { MockQueryResult.new(column1: 'nothing here...') }
        let(:results) { [first_result, second_result] }

        it { is_expected.to eq([['hello'], ['nothing here...']]) }
      end
    end

    describe '#each' do
      it 'should pass the block to the result of the query' do
        results = nil
        subject.each { |row| results = row }
        expect(results).to eq('results')
      end

      context 'when no block provided' do
        it 'should return an enumerator' do
          expect(subject.each).to be_a_kind_of(Enumerator)
        end
      end
    end

    describe '#each_slice' do
      it 'should pass the block to the result of the query' do
        results = nil
        subject.each_slice { |rows| results = rows }
        expect(results).to eq(%w(results))
      end

      context 'when no block provided' do
        it 'should return an enumerator' do
          expect(subject.each_slice).to be_a_kind_of(Enumerator)
        end
      end

      context 'with a slice size specified' do
        it 'should use the slice as the page size' do
          expect(record).to receive(:request_async).with({}, page_size: 5000)
          subject.each_slice(5000) {}
        end

        it 'should support different page sizes' do
          expect(record).to receive(:request_async).with({}, page_size: 3500)
          subject.each_slice(3500) {}
        end
      end
    end

    describe '#where' do
      let(:params) { { partition: 'Partition Key' } }

      it_behaves_like 'a method returning the builder', :where

      shared_examples_for 'a where query' do |request_method, query_method|
        it 'should forward the request' do
          expect(record).to receive(request_method).with(params, {})
          subject.where(params).send(query_method)
        end

        it 'should be able to chain requests' do
          expect(record).to receive(request_method).with(params.merge(cluster: 'Cluster Key'), {})
          subject.where(params).where(cluster: 'Cluster Key').send(query_method)
        end
      end

      it_behaves_like 'a where query', :request_async, :async
      it_behaves_like 'a where query', :request, :get
      it_behaves_like 'a where query', :first_async, :first_async
      it_behaves_like 'a where query', :first, :first
    end

    shared_examples_for 'an option query' do |method, option, request_method, query_method|
      it 'should forward the request' do
        expect(record).to receive(request_method).with({}, option => params)
        subject.send(method, *params).send(query_method)
      end

      it 'should be able to chain requests' do
        expect(record).to receive(request_method).with({cluster: 'Cluster Key'}, option => params)
        subject.send(method, *params).where(cluster: 'Cluster Key').send(query_method)
      end
    end

    shared_examples_for 'a comma separated option' do |method, option|
      describe "##{method}" do
        let(:params) { [:partition, :cluster] }

        it_behaves_like 'a method returning the builder', method

        it_behaves_like 'an option query', method, option, :request_async, :async
        it_behaves_like 'an option query', method, option, :request, :get
        it_behaves_like 'an option query', method, option, :first_async, :first_async
        it_behaves_like 'an option query', method, option, :first, :first

        it "should be able to chain #{method}s asynchronously" do
          expect(record).to receive(:request_async).with({}, option => params)
          subject.send(method, params[0]).send(method, params[1]).async
        end

        it "should be able to chain #{method}s immediately" do
          expect(record).to receive(:request).with({}, option => params)
          subject.send(method, params[0]).send(method, params[1]).get
        end

      end

    end

    it_behaves_like 'a comma separated option', :select, :select
    it_behaves_like 'a comma separated option', :order, :order_by

    describe '#limit' do
      let(:params) { 100 }

      it_behaves_like 'a method returning the builder', :limit

      it_behaves_like 'an option query', :limit, :limit, :request_async, :async
      it_behaves_like 'an option query', :limit, :limit, :request, :get
    end

    describe '#paginate' do
      let(:params) { 5000 }

      it_behaves_like 'a method returning the builder', :paginate

      it_behaves_like 'an option query', :paginate, :page_size, :request_async, :async
      it_behaves_like 'an option query', :paginate, :page_size, :request, :get
    end

    describe '#to_cql' do
      let(:record) { Record }

      it 'should create the cql query from the specified restrictitions' do
        expected_cql = Record.request_meta({partition: 'Partition Key'}, limit: 100).first
        expect(subject.where(partition: 'Partition Key').limit(100).to_cql).to eq(expected_cql)
      end
    end

  end
end