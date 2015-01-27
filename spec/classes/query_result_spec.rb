require 'rspec'

module CassandraModel
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

    describe '#==' do
      it 'should be true when the attributes are the same' do
        first_result = QueryResult.create(hello: 'world')
        second_result = QueryResult.create(hello: 'world')
        expect(first_result).to eq(second_result)
      end

      it 'should be false when the attributes are different' do
        first_result = QueryResult.create(hello: 'world')
        second_result = QueryResult.create(goobye: 'cruel world')
        expect(first_result).not_to eq(second_result)
      end
    end
  end
end