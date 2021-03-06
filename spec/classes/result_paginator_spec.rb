require 'spec_helper'

module CassandraModel
  describe ResultPaginator do

    let(:first_page_results) { ['Record 1'] }
    let(:last_page) { true }
    let(:second_page_future) { nil }
    let(:execution_info) { 'EXECUTION' }
    let(:first_page) { MockPage.new(last_page, second_page_future, first_page_results, execution_info) }
    let(:first_page_future) { ThomasUtils::Future.value(first_page) }
    let(:model_klass) { Faker::Lorem.sentence }
    let(:paginator) { ResultPaginator.new(first_page_future, model_klass) { |result, execution_info| "Modified #{result} #{execution_info}" } }

    subject { paginator }

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

      describe 'recording the duration of the page request' do
        let(:duration) { rand }

        it 'should log the time it took the request to complete' do
          allow_any_instance_of(ThomasUtils::Observation).to receive(:on_timed).and_yield(nil, nil, duration, first_page_results, nil)
          expect(Logging.logger).to receive(:debug) do |&block|
            expect(block.call).to eq("#{model_klass} Load (Page 1 with count 1): #{duration * 1000}ms")
          end
          subject.each {}
        end
      end

      context 'with multiple pages' do
        let(:last_page) { false }
        let(:second_page_results) { ['Record 2'] }
        let(:second_execution_info) { 'EXEC 2' }
        let(:second_page) { MockPage.new(true, nil, second_page_results, second_execution_info) }
        let(:second_page_future) { Cassandra::Future.value(second_page) }

        it 'should yield the results from both pages' do
          results = []
          subject.each do |result|
            results << result
          end
          expect(results).to eq(['Modified Record 1 EXECUTION', 'Modified Record 2 EXEC 2'])
        end

        describe 'recording the duration of the page request' do
          let(:duration) { rand }

          it 'should log the time it took the request to complete' do
            found_log = false
            allow_any_instance_of(ThomasUtils::Observation).to receive(:on_timed).and_yield(nil, nil, duration, second_page_results, nil)
            allow(Logging.logger).to receive(:debug) do |&block|
              found_log ||= block.call == "#{model_klass} Load (Page 2 with count 1): #{duration * 1000}ms"
            end
            subject.each {}
            expect(found_log).to eq(true)
          end
        end
      end
    end

    describe '#with_index' do
      let(:first_page_results) { Faker::Lorem.words }
      let(:paginator) { ResultPaginator.new(first_page_future, model_klass) { |result, _| result } }

      subject { paginator.with_index.to_a }

      it { is_expected.to eq(first_page_results.each.with_index.to_a) }

      context 'with a block given' do
        let(:results) { [] }
        subject { results }

        before { paginator.with_index { |*result| results << result } }

        it { is_expected.to eq(first_page_results.each.with_index.to_a) }
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
