require 'rspec'

#module Cassandra
#  class Future
#    class Ione < Struct.new(:future)
#      extend Forwardable
#
#      def_delegators :future, :on_failure, :on_complete, :then, :get
#      def_delegator :future, :on_value, :on_success
#
#      def add_listener(*_)
#        raise NotImplementedError
#      end
#
#      def promise
#        raise NotImplementedError
#      end
#
#      def join
#        get
#        self
#      end
#
#    end
#  end
#end

module CassandraModel
  class BatchReactor
    describe Future do

      let(:future) { error ? Ione::Future.failed(error) : Ione::Future.resolved(value) }
      let(:error) { nil }
      let(:value) { 56 }

      subject { Future.new(future) }

      describe 'delegating Cassandra::Future methods to an Ione::Future' do

        describe '#on_complete' do
          it 'should delegate #on_complete' do
            resolved_value = nil; subject.on_complete { |value, _| resolved_value = value }
            expect(resolved_value).to eq(value)
          end

          context 'when the future has been failed' do
            let(:error) { StandardError.new('Failed it :(') }

            it 'should delegate #on_complete' do
              resolved_error = nil; subject.on_complete { |_, error| resolved_error = error }
              expect(resolved_error).to eq(error)
            end
          end
        end

        describe '#on_failure' do
          let(:error) { StandardError.new('Failed it :(') }

          it 'should delegate #on_failure' do
            resolved_error = nil; subject.on_failure { |error| resolved_error = error }
            expect(resolved_error).to eq(error)
          end
        end

        describe '#on_success' do
          it 'should delegate to #on_value' do
            resolved_value = nil; subject.on_success { |value| resolved_value = value }
            expect(resolved_value).to eq(value)
          end
        end

        it 'should delegate #get' do
          expect(subject.get).to eq(56)
        end

        it 'should delegate #then' do
          expect(subject.then { |value| value +1 }.get).to eq(57)
        end

        describe '#join' do
          it 'should call #get on the future' do
            expect(future).to receive(:get).and_call_original
            subject.join
          end

          it 'should return itself' do
            expect(subject.join).to eq(subject)
          end
        end

      end

      describe 'methods not implemented' do
        it { expect { subject.promise }.to raise_error(NotImplementedError) }
        it { expect { subject.add_listener(double(:listener)) }.to raise_error(NotImplementedError) }
        it { expect { subject.fallback {} }.to raise_error(NotImplementedError) }
      end

    end
  end
end