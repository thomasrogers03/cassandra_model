require 'spec_helper'

module CassandraModel
  describe GlobalCallbacks do
    class GlobalCallbacks
      def self.reset!
        @listeners = nil
      end
    end

    let(:listener) { double(:callback) }

    before { GlobalCallbacks.add_listener(listener) }
    after { GlobalCallbacks.reset! }

    describe '.call' do
      it 'should call the callback on the listener with the specified params' do
        expect(listener).to receive(:on_save).with(:record_data)
        GlobalCallbacks.call(:save, :record_data)
      end

      context 'with a different callback' do
        it 'should call the callback on the listener with the specified params' do
          expect(listener).to receive(:on_error).with(:error, :message)
          GlobalCallbacks.call(:error, :error, :message)
        end
      end

      context 'with multiple listeners' do
        let(:listener) { double(:callback, on_error: nil) }
        let(:listener_two) { double(:callback) }
        before { GlobalCallbacks.add_listener(listener_two) }

        it 'should run the callback for each listener' do
          expect(listener_two).to receive(:on_error).with(:error, :message)
          GlobalCallbacks.call(:error, :error, :message)
        end
      end

      context 'when the listener does not respond to the callback' do
        it 'should not call the method' do
          expect { GlobalCallbacks.call(:error, :error, :message) }.not_to raise_error
        end
      end
    end

    describe 'dynamic callbacks' do
      let(:helper) { double(:callback) }

      it 'should allow us to define callbacks using on_callback' do
        GlobalCallbacks.on_save { |data| helper.save(data) }
        expect(helper).to receive(:save).with(:data)
        GlobalCallbacks.call(:save, :data)
      end

      context 'with a different callback' do
        it 'should allow us to define callbacks using on_callback' do
          GlobalCallbacks.on_error { |record, error| helper.fail(record, error) }
          expect(helper).to receive(:fail).with(:record, :error)
          GlobalCallbacks.call(:error, :record, :error)
        end
      end
    end

  end
end
