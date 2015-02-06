require 'rspec'

describe QueryBuilder do
  let(:results) { %w(results) }
  let(:record) { double(:record, request_async: nil, request: results) }

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

  describe '#select' do
    let(:params) { [:partition, :cluster] }

    it_behaves_like 'a method returning the builder', :select

    it_behaves_like 'an option query', :select, :select, :request_async, :async
    it_behaves_like 'an option query', :select, :select, :request, :get
    it_behaves_like 'an option query', :select, :select, :first_async, :first_async
    it_behaves_like 'an option query', :select, :select, :first, :first

    it 'should be able to chain selects asynchronously' do
      expect(record).to receive(:request_async).with({}, select: params)
      subject.select(params[0]).select(params[1]).async
    end

    it 'should be able to chain selects immediately' do
      expect(record).to receive(:request).with({}, select: params)
      subject.select(params[0]).select(params[1]).get
    end

  end

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

end