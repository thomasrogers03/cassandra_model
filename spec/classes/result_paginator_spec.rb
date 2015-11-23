require 'rspec'

module CassandraModel
  describe ResultPaginator do
    let(:first_page_results) { ['Record 1'] }
    let(:last_page) { true }
    let(:second_page_future) { nil }
    let(:execution_info) { 'EXECUTION' }
    let(:first_page) { MockPage.new(last_page, second_page_future, first_page_results, execution_info) }
    let(:first_page_future) { double(:result, get: first_page) }
    subject { ResultPaginator.new(first_page_future) { |result, execution_info| "Modified #{result} #{execution_info}" } }

    it { should be_a_kind_of(Enumerable) }

    describe '#each' do
      it 'should yield the modified results of the page' do
        expect { |block| subject.each(&block) }.to yield_with_args('Modified Record 1 EXECUTION')
      end

      context 'when no block provided' do
        it 'should return an enumerator' do
          expect(subject.each).to be_a_kind_of(Enumerator)
        end
      end

      context 'with multiple results in the first page' do
        let(:first_page_results) { ['Record 1', 'Record 2'] }

        it 'should yield both results' do
          results = []
          subject.each do |result|
            results << result
          end
          expect(results).to eq(['Modified Record 1 EXECUTION', 'Modified Record 2 EXECUTION'])
        end
      end

      context 'with multiple pages' do
        let(:last_page) { false }
        let(:second_page_results) { ['Record 2'] }
        let(:second_execution_info) { 'EXEC 2' }
        let(:second_page) { MockPage.new(true, nil, second_page_results, second_execution_info) }
        let(:second_page_future) { double(:result, get: second_page) }

        it 'should yield the results from both pages' do
          results = []
          subject.each do |result|
            results << result
          end
          expect(results).to eq(['Modified Record 1 EXECUTION', 'Modified Record 2 EXEC 2'])
        end
      end
    end

    describe '#each_slice' do
      context 'when no block provided' do
        it 'should return an enumerator' do
          expect(subject.each_slice).to be_a_kind_of(Enumerator)
        end
      end

      context 'with an empty result set' do
        let(:first_page_results) { [] }
        it 'should not yield the block' do
          expect { |b| subject.each_slice(&b) }.to_not yield_control
        end
      end
    end

    describe '#get' do
      it 'should delegate to #to_a' do
        expect(subject.get).to eq(subject.to_a)
      end
    end
  end
end
