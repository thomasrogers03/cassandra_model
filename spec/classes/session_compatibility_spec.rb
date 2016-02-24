require 'rspec'

module Cassandra
  describe Session do

    subject { Session.new(nil, nil, nil) }

    describe '#execute_async' do
      let(:result) { [] }
      let(:statement) { Faker::Lorem.sentence }
      let(:args) { Faker::Lorem.words }
      let(:options) { {Faker::Lorem.word => Faker::Lorem.word} }

      before do
        allow(subject).to receive(:__execute_async) do |statement, options|
          result << [statement, options]
        end
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

    end

  end
end
