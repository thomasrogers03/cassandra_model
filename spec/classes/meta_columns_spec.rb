require 'rspec'

module CassandraModel
  describe MetaColumns do

    class MockRecord
      extend MetaColumns

      attr_reader :attributes

      def initialize(attributes)
        @attributes = attributes
        MockRecord.after_initialize(self)
      end

      def self.reset!
        @deferred_column_writers = nil
        @async_deferred_column_readers = nil
        @async_deferred_column_writers = nil
      end

      def save
        MockRecord.after_save(self)
      end

      def save_async
        MockRecord.after_save_async(self)
      end
    end

    subject { MockRecord }

    before { MockRecord.reset! }

    shared_examples_for 'a record defining meta columns' do
      let(:on_load) { ->(attributes) { "#{attributes[:partition]} World" } }
      let(:on_load_async) do
        ->(attributes) do
          MockFuture.new("#{attributes[:partition]} World")
        end
      end
      let(:attributes) { { partition: 'Hello' } }

      shared_examples_for 'a method defining meta columns' do |method|
        describe ".#{method}" do

          it 'should define a method to load a deferred column based on the record attributes' do
            subject.send(method, :data, on_load: on_load)
            expect(subject.new(attributes).data).to eq('Hello World')
          end

          it 'should define a method to overwrite the value' do
            subject.send(method, :data, on_load: on_load)
            record = subject.new(attributes)
            record.data = 'Goodbye World'
            expect(record.data).to eq('Goodbye World')
          end

          it 'should define a method to save a deferred column based on the record attributes' do
            data = nil
            on_save = ->(attributes, value) { data = "#{value}, #{attributes[:partition]}" }
            subject.send(method, :data, on_load: on_load, on_save: on_save)
            subject.new(attributes).save_data
            expect(data).to eq('Hello World, Hello')
          end

          it 'should not define a method for saving when no on_save is set' do
            record = subject.new(attributes)
            MockRecord.send(:remove_method, :save_data) if record.public_methods.include?(:save_data)
            subject.send(method, :data, on_load: on_load)
            expect(record).not_to respond_to(:save_data)
          end

          it 'should raise an error when no on_load method is provided' do
            expect { subject.send(method, :data, {}) }.to raise_error('No on_load method provided')
          end
        end
      end

      context 'with synchronous deferred columns' do
        it_behaves_like 'a method defining meta columns', :deferred_column

        it 'should only call the block once' do
          on_load = double(:proc, call: nil)
          subject.deferred_column(:data, on_load: on_load)
          record = subject.new(attributes)

          record.data
          expect(on_load).not_to receive(:call)
          record.data
        end
      end

      context 'with a asynchronous deferred columns' do
        let(:on_load) { on_load_async }
        it_behaves_like 'a method defining meta columns', :async_deferred_column

        it 'should immediately begin loading the deferred column on post-initialization' do
          on_load = double(:proc, call: nil)
          subject.async_deferred_column(:data, on_load: on_load)

          record = subject.new(attributes)
          expect(on_load).not_to receive(:call)
          record.data
        end
      end

      describe '#save' do
        context 'with synchronous deferred columns' do
          it 'should save the deferred columns' do
            on_save = double(:proc)
            subject.deferred_column(:data, on_load: on_load, on_save: on_save)
            expect(on_save).to receive(:call).with(attributes, 'Hello World')
            subject.new(attributes).save
          end
        end

        context 'with asynchronous deferred columns' do
          let(:on_load) { on_load_async }
          let(:future) { MockFuture.new('OK') }

          it 'should save the deferred columns' do
            on_save = double(:proc, call: future)
            subject.async_deferred_column(:data, on_load: on_load, on_save: on_save)
            expect(subject.new(attributes).save).to eq(%w(OK))
          end
        end
      end

      describe '#save_async' do
        let(:on_load) { on_load_async }
        let(:future) { MockFuture.new('OK') }

        it 'should save the deferred columns' do
          on_save = double(:proc, call: future)
          subject.async_deferred_column(:data, on_load: on_load, on_save: on_save)
          expect(subject.new(attributes).save_async.map(&:get)).to eq(%w(OK))
        end
      end

    end

    it_behaves_like 'a record defining meta columns'

  end
end