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
    let(:single_result_future) { MockFuture.new(results.first) }
    let(:create_result) { double(:record) }
    let(:create_result_future) { MockFuture.new(create_result) }
    let(:record) do
      double(:record_klass, request_async: result_paginator, request: results,
             first_async: single_result_future, first: results.first,
             create_async: create_result_future, create: create_result,
             request_cql: nil)
    end

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
      it 'should execute the built query asynchronously' do
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

    describe '#create_async' do
      it 'should execute the built query asynchronously' do
        expect(record).to receive(:create_async).with({partition_key: 'Partition', cluster_key: 'Cluster'}, check_exists: true)
        subject.create_async({partition_key: 'Partition', cluster_key: 'Cluster'}, check_exists: true)
      end

      context 'when called without options' do
        it 'should create a record with the specified attributes' do
          expect(record).to receive(:create_async).with({partition_key: 'Partition'}, {})
          subject.create_async(partition_key: 'Partition')
        end
      end

      context 'when called without any arguments' do
        it 'should create a record with attributes inherited from the current builder state' do
          expect(record).to receive(:create_async).with({partition_key: 'Partition'}, {})
          subject.where(partition_key: 'Partition').create_async
        end
      end
    end

    describe '#create' do
      it 'should execute the built query' do
        expect(record).to receive(:create).with({partition_key: 'Partition', cluster_key: 'Cluster'}, check_exists: true)
        subject.create({partition_key: 'Partition', cluster_key: 'Cluster'}, check_exists: true)
      end

      context 'when called without options' do
        it 'should create a record with the specified attributes' do
          expect(record).to receive(:create).with({partition_key: 'Partition'}, {})
          subject.create(partition_key: 'Partition')
        end
      end

      context 'when called without any arguments' do
        it 'should create a record with attributes inherited from the current builder state' do
          expect(record).to receive(:create).with({partition_key: 'Partition'}, {})
          subject.where(partition_key: 'Partition').create
        end
      end
    end

    describe '#new' do
      let(:record) { Record }
      let(:record_attributes) do
        {partition: 'Partition', clustering: 'Clustering', meta_data: 'Fake'}
      end

      before { mock_simple_table(:records, [:partition], [:clustering], [:meta_data]) }

      it 'should create an instance of the record' do
        expect(subject.new(record_attributes)).to eq(Record.new(record_attributes))
      end

      context 'with different attributes' do
        let(:record_attributes) do
          {partition: 'Updated Partition', clustering: 'Updated Clustering', meta_data: 'No Data'}
        end

        it 'should create an instance of the record' do
          expect(subject.new(record_attributes)).to eq(Record.new(record_attributes))
        end
      end
    end

    describe '#check_exists' do
      context 'when used with #create_async' do
        it 'should append an option to check the existence of a record' do
          expect(record).to receive(:create_async).with({partition_key: 'Partition'}, check_exists: true)
          subject.check_exists.where(partition_key: 'Partition').create_async
        end
      end

      context 'when used with #create' do
        it 'should append an option to check the existence of a record' do
          expect(record).to receive(:create).with({partition_key: 'Partition'}, check_exists: true)
          subject.check_exists.where(partition_key: 'Partition').create
        end
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
      let(:params) { {partition: 'Partition Key'} }

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

    describe '#inspect' do
      let(:record) { Record }
      let(:attributes) { {partition: 'Partition'} }
      let(:options) { {limit: 10} }
      let(:results) { %w(result1 result2 result3) }
      let(:expected_result) { "CassandraModel::QueryBuilder: #{results.map(&:inspect) + %w(...)}" }

      subject { QueryBuilder.new(record).where(attributes).inspect }

      before { allow(Record).to receive(:request).with(attributes, options).and_return(results) }

      it { is_expected.to eq(expected_result) }

      context 'with different attributes' do
        let(:attributes) { {partition: 'Partition'} }
        let(:results) { %w(image1 image2 image3) }

        it { is_expected.to eq(expected_result) }
      end

      context 'when the limit is overridden' do
        let(:options) { {limit: 100} }
        subject { QueryBuilder.new(record).where(attributes).limit(100).inspect }

        it { is_expected.to eq(expected_result) }
      end
    end

  end
end