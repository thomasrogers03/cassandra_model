require 'rspec'

describe QueryResult do
  class QueryResult
    def self.reset!
      @@klass_cache = {}
    end
  end

  before { QueryResult.reset! }

  describe '.create' do
    it 'should create a class with read only attributes for the query result' do
      expect(QueryResult.create(hello: 'world').hello).to eq('world')
    end

    context 'with different attributes' do
      it 'should create a class with read only attributes for the query result' do
        expect(QueryResult.create(goodbye: 'cruel world').goodbye).to eq('cruel world')
      end
    end

    it 'should cache created classes by their columns' do
      first_class = QueryResult.create(hello: 'world').class
      second_class = QueryResult.create(hello: 'world').class
      expect(first_class).to eq(second_class)
    end
  end
end