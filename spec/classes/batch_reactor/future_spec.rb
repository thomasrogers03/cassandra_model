require 'rspec'

module CassandraModel
  class BatchReactor
    describe Future do

      let(:future) { error ? Ione::Future.failed(error) : Ione::Future.resolved(value) }
      let(:error) { nil }
      let(:value) { 56 }

      subject { Future.new(future) }

      it { is_expected.to be_a_kind_of(Cassandra::Future) }

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

          it 'should return the future' do
            expect(subject.on_failure {}).to eq(subject)
          end
        end

        describe '#on_failure' do
          let(:error) { StandardError.new('Failed it :(') }

          it 'should delegate #on_failure' do
            resolved_error = nil; subject.on_failure { |error| resolved_error = error }
            expect(resolved_error).to eq(error)
          end

          it 'should return the future' do
            expect(subject.on_failure {}).to eq(subject)
          end
        end

        describe '#on_success' do
          it 'should delegate to #on_value' do
            resolved_value = nil; subject.on_success { |value| resolved_value = value }
            expect(resolved_value).to eq(value)
          end

          it 'should return the future' do
            expect(subject.on_failure {}).to eq(subject)
          end
        end

        it 'should delegate #get' do
          expect(subject.get).to eq(56)
        end

        describe '#then' do
          it 'should delegate #then' do
            expect(subject.then { |value| value +1 }.get).to eq(57)
          end

          it 'should return a BatchReactor::Future' do
            expect(subject.then {}).to be_a_kind_of(Future)
          end
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

      describe '#add_listener' do
        let(:listener) { double(:listener, success: nil, failure: nil) }

        it 'should return itself' do
          expect(subject.add_listener(listener)).to eq(subject)
        end

        it 'should call #success on the listener with the value' do
          expect(listener).to receive(:success).with(value)
          subject.add_listener(listener)
        end

        context 'with an error' do
          let(:error) { StandardError.new('It died...') }

          it 'should call #failure on the listener with the error' do
            expect(listener).to receive(:failure).with(error)
            subject.add_listener(listener)
          end
        end

      end

      describe 'methods not implemented' do
        it { expect { subject.promise }.to raise_error(NotImplementedError) }
        it { expect { subject.fallback {} }.to raise_error(NotImplementedError) }
      end

    end
  end
end