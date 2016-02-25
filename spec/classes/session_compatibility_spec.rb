require 'rspec'

module Cassandra
  describe Session do

    let(:result) { [] }
    let(:statement) { Faker::Lorem.sentence }
    let(:args) { Faker::Lorem.words }
    let(:options) { {Faker::Lorem.word => Faker::Lorem.word} }
    let(:result_future) { Cassandra::Future.value(result) }

    subject { Session.new(nil, nil, nil) }

    before do
      allow(subject).to receive(:__execute_async) do |statement, options|
        result << [statement, options]
        result_future
      end
    end

    describe '#execute_async' do
      it 'should return a future resolving to the result' do
        expect(subject.execute_async(statement).get).to eq(result)
      end

      it 'should support querying with no args' do
        subject.execute_async(statement)
        expect(result[0][0]).to eq(statement)
      end

      it 'should support splat args' do
        subject.execute_async(statement, *args)
        expect(result[0][1]).to eq(arguments: args)
      end

      it 'supports appending an options hash' do
        subject.execute_async(statement, *args, options)
        expect(result[0][1]).to eq(options.merge(arguments: args))
      end

      context 'when using the new interface properly' do
        it 'allows us to pass in arguments' do
          subject.execute_async(statement, options.merge(arguments: args))
          expect(result[0][1]).to eq(options.merge(arguments: args))
        end
      end
    end

    describe '#execute' do
      it 'should resolve the future' do
        expect(subject.execute(statement)).to eq(result)
      end

      it 'should support querying with no args' do
        subject.execute(statement)
        expect(result[0][0]).to eq(statement)
      end

      it 'should support splat args' do
        subject.execute(statement, *args)
        expect(result[0][1]).to eq(arguments: args)
      end

      it 'supports appending an options hash' do
        subject.execute(statement, *args, options)
        expect(result[0][1]).to eq(options.merge(arguments: args))
      end
    end

  end
end
