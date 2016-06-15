require 'rspec'

module CassandraModel
  describe Observable do

    let(:value) { Faker::Lorem.sentence }
    let(:error) { nil }
    let(:future) { error ? Cassandra::Future.error(error) : Cassandra::Future.value(value) }
    let(:observable) { Observable.new(future) }
    let(:observer) { double(:observer, update: nil) }

    subject { observable }

    describe '.create_observation' do
      subject { Observable.create_observation(future) }

      it { is_expected.to be_a_kind_of(ThomasUtils::Observation) }
      its(:get) { is_expected.to eq(value) }
    end

    describe '#value' do
      its(:value) { is_expected.to eq(value) }

      context 'with an error' do
        let(:error) { StandardError.new(Faker::Lorem.sentence) }

        its(:value) { is_expected.to eq(nil) }

        context 'with a non-standard error' do
          let(:error) { Interrupt.new }

          its(:value) { is_expected.to eq(nil) }
        end
      end
    end

    describe '#value!' do
      its(:value!) { is_expected.to eq(value) }

      context 'with an error' do
        let(:error_description) { Faker::Lorem.sentence }
        let(:error) { StandardError.new(error_description) }

        it { expect { subject.value! }.to raise_error(StandardError, error_description) }
      end
    end

    shared_examples_for 'a method adding an observer' do |method|
      around { |example| Timecop.freeze { example.run } }

      context 'with an observer' do
        it 'should call update on the observer with the result' do
          expect(observer).to receive(:update).with(Time.now, value, nil)
          subject.public_send(method, observer)
        end

        context 'with a different function specified' do
          let(:function) { Faker::Lorem.word.to_sym }

          it 'should the specified function' do
            expect(observer).to receive(function).with(Time.now, value, nil)
            subject.public_send(method, observer, function)
          end
        end

        context 'with an error' do
          let(:error) { StandardError.new(Faker::Lorem.sentence) }

          it 'should call update on the observer with the error' do
            expect(observer).to receive(:update).with(Time.now, nil, error)
            subject.public_send(method, observer)
          end
        end

        context 'with a block' do
          let(:block) { lambda { |time, value, error| observer.update(time, value, error) } }

          it 'should execute the block with the result' do
            expect(observer).to receive(:update).with(Time.now, value, nil)
            subject.public_send(method, &block)
          end
        end
      end
    end

    describe '#add_observer' do
      it_behaves_like 'a method adding an observer', :add_observer
    end

    describe '#with_observer' do
      it_behaves_like 'a method adding an observer', :with_observer

      it 'should return the observable' do
        expect(subject.with_observer(observer)).to eq(subject)
      end
    end

    shared_examples_for 'an unimplemented method' do
      it { expect { subject }.to raise_error(NotImplementedError) }
    end

    describe '#delete_observer' do
      subject { observable.delete_observer(observer) }
      it_behaves_like 'an unimplemented method'
    end

    describe '#delete_observers' do
      subject { observable.delete_observers }
      it_behaves_like 'an unimplemented method'
    end

    describe '#count_observers' do
      subject { observable.count_observers }
      it_behaves_like 'an unimplemented method'
    end

  end
end
