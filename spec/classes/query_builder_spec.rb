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
    let(:record_scopes) { {} }
    let(:record_primary_key) { [] }
    let(:predecessor_record) { nil }
    let(:record) do
      double(:record_klass, request_async: result_paginator, request: results,
             first_async: single_result_future, first: results.first,
             create_async: create_result_future, create: create_result,
             scopes: record_scopes,
             primary_key: record_primary_key,
             predecessor: predecessor_record,
             request_cql: nil)
    end

    subject { QueryBuilder.new(record) }

    it { is_expected.to be_a_kind_of(Enumerable) }

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
      let(:results) { Faker::Lorem.words }
      let(:params) { Faker::Lorem.words }
      let(:options) { Faker::Lorem.words }

      subject { QueryBuilder.new(record, params, options) }

      it 'should execute the built query asynchronously' do
        expect(record).to receive(:first_async).with(params, options)
        subject.first_async
      end

      context 'with a predecessor record class' do
        let(:predecessor_record) { double(:record_klass) }

        before do
          allow(record).to receive(:first_async).with(params, options).and_return(Cassandra::Future.value(nil))
          allow(predecessor_record).to receive(:first_async).with(params, options).and_return(single_result_future)
        end

        it 'should grab the first available record' do
          expect(subject.first).to eq(results.first)
        end
      end
    end

    describe '#first' do
      let(:results) { Faker::Lorem.words }

      it 'should return the resolved result of #first_async' do
        expect(subject.first).to eq(results.first)
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
      after { Record.reset! }

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

      context 'with attributes specified through the where clause' do
        subject { QueryBuilder.new(record).where(record_attributes) }

        it 'should create an instance of the record with those attributes' do
          expect(subject.new({})).to eq(Record.new(record_attributes))
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

    describe '#first_or_new_async' do
      let(:results) { [] }
      let(:record_attributes) do
        {partition: 'Partition', clustering: 'Clustering'}
      end

      before { mock_simple_table(:records, [:partition], [:clustering], [:meta_data]) }
      after { Record.reset! }

      before do
        allow(record).to receive(:new) do |attributes|
          Record.new(attributes)
        end
      end

      subject { QueryBuilder.new(record).where(record_attributes).first_or_new_async({}).get }

      it 'should create a new instance of the record' do
        is_expected.to eq(Record.new(record_attributes))
      end

      context 'when the record already exists' do
        let(:result_attributes) { record_attributes.merge(meta_data: 'Here I am') }
        let(:first_result) { MockQueryResult.new(result_attributes) }
        let(:results) { [first_result] }

        it 'should return the existing record' do
          expect(subject.attributes).to eq(result_attributes)
        end
      end
    end

    describe '#first_or_new' do
      let(:attributes) { {partition: 'Key'} }
      let(:future_result) { Cassandra::Future.value(:new_record) }

      subject { QueryBuilder.new(record).first_or_new(attributes) }

      before do
        allow_any_instance_of(QueryBuilder).to receive(:first_or_new_async).with(attributes).and_return(future_result)
      end

      it { is_expected.to eq(:new_record) }
    end

    describe '#pluck' do
      let(:first_result) { MockQueryResult.new(column1: 'hello', column2: 'world', column3: 'good bye!') }
      let(:results) { [first_result] }
      let(:pluck_columns) { [:column1] }

      subject { QueryBuilder.new(record).pluck(*pluck_columns) }

      it 'should grab the columns from the resulting records' do
        is_expected.to eq(%w(hello))
      end

      context 'with different pluck columns' do
        let(:pluck_columns) { [:column2, :column3] }

        it { is_expected.to eq([['world', 'good bye!']]) }
      end

      context 'with multiple results' do
        let(:pluck_columns) { [:column1, :column2] }
        let(:second_result) { MockQueryResult.new(column1: 'nothing here...', column2: 'really nothing...') }
        let(:results) { [first_result, second_result] }

        it { is_expected.to eq([%w(hello world), ['nothing here...', 'really nothing...']]) }
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
          expect(subject.each).to be_a_kind_of(Enumerable)
        end
      end

      context 'when the record class has a predecessor' do
        let(:predecessor_record) { double(:record_klass) }
        let(:params) { {} }
        let(:options) { {} }
        let(:extra_options) { {result_modifiers: []} }
        let(:query_builder) { QueryBuilder.new(record, params, options, extra_options) }
        let(:query_builder_two) { double(:query_builder) }
        let(:query_builder_three) do
          QueryBuilder.new(record, params, options, extra_options.merge(skip_predecessor: true, result_modifiers: []))
        end
        let(:combiner_enum) { double(:combiner_enum) }
        let(:combiner) { double(:combiner) }
        let(:block_result) { double(:proc) }
        let(:block) { ->() { block_result } }

        subject { QueryBuilder.new(record).each(&block) }

        before do
          allow(QueryBuilder).to receive(:new).and_call_original
          allow(QueryBuilder).to receive(:new).with(predecessor_record, params, options, extra_options).and_return(query_builder_two)
          allow(ResultCombiner).to receive(:new).with(query_builder_three, query_builder_two).and_return(combiner)
          allow(combiner).to(receive(:each)) do |&block|
            expect(block.call).to eq(block_result)
            combiner_enum
          end
        end

        it { is_expected.to eq(combiner_enum) }
      end
    end

    describe '#cluster' do
      let(:results_klass) { Struct.new(:attributes) }
      let(:results) { [results_klass.new(Faker::Lorem.word => Faker::Lorem.sentence)] }
      let(:cluster_key) { Faker::Lorem.word.to_sym }
      let(:cluster_key_two) { Faker::Lorem.word.to_sym }
      let(:params) { {cluster_key => Faker::Lorem.sentence, cluster_key_two => Faker::Lorem.sentence} }
      let(:cluster) { [cluster_key, cluster_key_two] }

      it 'changes the enumerator provided by #each to a ResultChunker' do
        enum = subject.where(params).cluster(*cluster).each
        expect(enum).to eq(ResultChunker.new(result_paginator, cluster))
      end

      it 'makes #each_slice raise a NotImplementedError' do
        expect { subject.where(params).cluster(*cluster).each_slice(500) }.to raise_error(NotImplementedError)
      end

      it 'makes #first_async raise a NotImplementedError' do
        expect { subject.where(params).cluster(*cluster).first_async }.to raise_error(NotImplementedError)
      end

      describe 'the results' do
        let(:enum_results) { [] }
        before { subject.where(params).cluster(*cluster).each { |cluster, rows| enum_results << [cluster, rows] } }

        it 'supports passing a block' do
          expect(enum_results).to eq(ResultChunker.new(result_paginator, cluster).to_a)
        end
      end

      context 'when a limit is provided after the cluster' do
        let(:limit) { rand(1..10) }

        it 'changes the enumerator provided by #each to a ResultChunker wrapped in a ResultLimiter' do
          enum = subject.where(params).cluster(*cluster).limit(limit).each
          expect(enum).to eq(ResultLimiter.new(ResultChunker.new(result_paginator, cluster), limit))
        end

        it 'should not limit the query' do
          expect(record).to receive(:request_async).with(params, {}).and_return(result_paginator)
          subject.where(params).cluster(*cluster).limit(limit).each
        end
      end
    end

    describe '#filter' do
      let(:results_klass) { Struct.new(:attributes) }
      let(:results) { [results_klass.new(Faker::Lorem.word => Faker::Lorem.sentence)] }
      let(:filter_block) { ->(_) { true } }
      let(:cluster_key) { Faker::Lorem.word.to_sym }
      let(:cluster_key_two) { Faker::Lorem.word.to_sym }
      let(:params) { {cluster_key => Faker::Lorem.sentence, cluster_key_two => Faker::Lorem.sentence} }

      it 'changes the enumerator provided by #each to a ResultChunker' do
        enum = subject.where(params).filter(&filter_block).each
        expect(enum).to eq(ResultFilter.new(result_paginator, filter_block))
      end

      it 'makes #each_slice raise a NotImplementedError' do
        expect { subject.where(params).filter(&filter_block).each_slice(753) }.to raise_error(NotImplementedError)
      end

      it 'makes #first_async raise a NotImplementedError' do
        expect { subject.where(params).filter(&filter_block).first_async }.to raise_error(NotImplementedError)
      end

      describe 'the results' do
        let(:enum_results) { [] }
        before { subject.where(params).filter(&filter_block).each { |rows| enum_results << rows } }

        it 'supports passing a block' do
          expect(enum_results).to eq(ResultFilter.new(result_paginator, filter_block).to_a)
        end
      end

      context 'when a limit is provided after the filter' do
        let(:limit) { rand(1..10) }

        it 'changes the enumerator provided by #each to a ResultChunker wrapped in a ResultLimiter' do
          enum = subject.where(params).filter(&filter_block).limit(limit).each
          expect(enum).to eq(ResultLimiter.new(ResultFilter.new(result_paginator, filter_block), limit))
        end

        it 'should not limit the query' do
          expect(record).to receive(:request_async).with(params, {}).and_return(result_paginator)
          subject.where(params).filter(&filter_block).limit(limit).each
        end
      end
    end

    describe '#reduce_by_columns' do
      let(:results_klass) { Struct.new(:attributes) }
      let(:results) { [results_klass.new(Faker::Lorem.word => Faker::Lorem.sentence)] }
      let(:cluster_key) { Faker::Lorem.word.to_sym }
      let(:cluster_key_two) { Faker::Lorem.word.to_sym }
      let(:params) { {cluster_key => Faker::Lorem.sentence, cluster_key_two => Faker::Lorem.sentence} }
      let(:keys) { [cluster_key, cluster_key_two] }

      it 'changes the enumerator provided by #each to a ResultChunker' do
        enum = subject.where(params).reduce_by_columns(*keys).each
        expect(enum).to eq(ResultReducerByKeys.new(result_paginator, keys))
      end

      it 'makes #each_slice raise a NotImplementedError' do
        expect { subject.where(params).reduce_by_columns(*keys).each_slice(753) }.to raise_error(NotImplementedError)
      end

      it 'makes #first_async raise a NotImplementedError' do
        expect { subject.where(params).reduce_by_columns(*keys).first_async }.to raise_error(NotImplementedError)
      end

      describe 'the results' do
        let(:results_klass) { Struct.new(cluster_key, cluster_key_two) }
        let(:enum_results) { [] }
        before { subject.where(params).reduce_by_columns(*keys).each { |rows| enum_results << rows } }

        it 'supports passing a block' do
          expect(enum_results).to eq(ResultReducerByKeys.new(result_paginator, keys).to_a)
        end
      end

      context 'when a limit is provided after the filter' do
        let(:limit) { rand(1..10) }

        it 'changes the enumerator provided by #each to a ResultChunker wrapped in a ResultLimiter' do
          enum = subject.where(params).reduce_by_columns(*keys).limit(limit).each
          expect(enum).to eq(ResultLimiter.new(ResultReducerByKeys.new(result_paginator, keys), limit))
        end

        it 'should not limit the query' do
          expect(record).to receive(:request_async).with(params, {}).and_return(result_paginator)
          subject.where(params).reduce_by_columns(*keys).limit(limit).each
        end
      end
    end

    describe '#cluster_except' do
      let(:results) { [] }
      let(:record_primary_key) { Faker::Lorem.words.map(&:to_sym) }
      let(:key_size) { rand(0...record_primary_key.length) }
      let(:except_keys) { record_primary_key.sample(key_size) }
      let(:expected_query) do
        subject.cluster(*(record_primary_key - except_keys))
      end

      it 'delegates to #cluster with the remaining columns no included in the except keys' do
        expect(subject.cluster_except(*except_keys)).to eq(expected_query)
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

      shared_examples_for 'a where query' do |request_method, query_method|
        it 'should forward the request' do
          expect(record).to receive(request_method).with(params, {})
          subject.where(params).send(query_method)
        end

        it 'should be able to chain requests' do
          expect(record).to receive(request_method).with(params.merge(cluster: 'Cluster Key'), {})
          subject.where(params).where(cluster: 'Cluster Key').send(query_method)
        end

        it 'supports string keys' do
          expect(record).to receive(request_method).with(params, {})
          subject.where(params.stringify_keys).send(query_method)
        end
      end

      it_behaves_like 'a where query', :request_async, :async
      it_behaves_like 'a where query', :request, :get
      it_behaves_like 'a where query', :first_async, :first_async
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

        it_behaves_like 'an option query', method, option, :request_async, :async
        it_behaves_like 'an option query', method, option, :request, :get
        it_behaves_like 'an option query', method, option, :first_async, :first_async

        it "should be able to chain #{method}s asynchronously" do
          expect(record).to receive(:request_async).with({}, option => params)
          subject.send(method, params[0]).send(method, params[1]).async
        end

        it "should be able to chain #{method}s immediately" do
          expect(record).to receive(:request).with({}, option => params)
          subject.send(method, params[0]).send(method, params[1]).get
        end

        it "supports specifying string values to #{method}" do
          expect(record).to receive(:request).with({}, option => params)
          subject.send(method, params[0].to_s).send(method, params[1].to_s).get
        end

      end

    end

    it_behaves_like 'a comma separated option', :select, :select
    it_behaves_like 'a comma separated option', :order, :order_by

    describe '#select' do
      context 'when the columns are specified using a hash' do
        let(:first_column_key) { Faker::Lorem.word.to_sym }
        let(:second_column_key) { (Faker::Lorem.words * '_').to_sym }
        let(:first_column) { {first_column_key => :avg} }
        let(:second_column) { {second_column_key => :count} }
        let(:select_columns) do
          first_column.merge(second_column)
        end

        it 'should split the hash into multiple ordering clauses' do
          expect(record).to receive(:request).with({}, {select: [first_column, second_column]})
          subject.select(select_columns).get
        end

        it 'should convert the input column names to symbols' do
          expect(record).to receive(:request).with({}, {select: [first_column, second_column]})
          subject.select(select_columns.stringify_keys).get
        end
      end
    end

    describe '#order' do
      context 'when the order is specified using a hash' do
        let(:first_column) { {Faker::Lorem.word.to_sym => :asc} }
        let(:second_column) { {(Faker::Lorem.words * '_').to_sym => :desc} }
        let(:ordering_columns) do
          first_column.merge(second_column)
        end

        it 'should split the hash into multiple ordering clauses' do
          expect(record).to receive(:request).with({}, {order_by: [first_column, second_column]})
          subject.order(ordering_columns).get
        end
      end
    end

    describe '#limit' do
      let(:params) { 100 }

      it_behaves_like 'an option query', :limit, :limit, :request_async, :async
      it_behaves_like 'an option query', :limit, :limit, :request, :get
    end

    describe '#trace' do
      let(:params) { false }

      it_behaves_like 'an option query', :trace, :trace, :request_async, :async
      it_behaves_like 'an option query', :trace, :trace, :request, :get
    end

    describe '#paginate' do
      let(:params) { 5000 }

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
      let(:inspected_results) { results.map(&:to_s) * ', ' }
      let(:expected_result) { "#<CassandraModel::QueryBuilder: [#{inspected_results}, ...]>" }

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

    describe 'scoping' do
      let(:scope_name) { Faker::Lorem.word }
      let(:scope_args) { [] }

      subject { QueryBuilder.new(record).public_send(scope_name, *scope_args) }

      it { expect { subject }.to raise_error(NoMethodError) }

      context 'when the record provides a scope' do
        let(:key) { Faker::Lorem.word }
        let(:value) { Faker::Lorem.sentence }
        let(:scope_name) { Faker::Lorem.word.to_sym }
        let(:scope) do
          scope_key = key
          scope_value = value
          ->() { where(scope_key => scope_value) }
        end
        let(:record_scopes) { {scope_name => scope} }

        it { is_expected.to eq(QueryBuilder.new(record).where(key => value)) }

        context 'with a scope taking parameters' do
          let(:scope_args) { [key, value] }
          let(:scope) { ->(scope_key, scope_value) { where(scope_key => scope_value) } }

          it { is_expected.to eq(QueryBuilder.new(record).where(key => value)) }
        end
      end

    end

  end
end
