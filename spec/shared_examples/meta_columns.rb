module CassandraModel
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

        it 'should record the column name' do

          subject.send(method, :data, on_load: on_load)
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
          subject.send(:remove_method, :save_data) if record.public_methods.include?(:save_data)
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

    describe '.save_deferred_columns' do
      context 'with synchronous deferred columns' do
        it 'should save the deferred columns' do
          on_save = double(:proc)
          subject.deferred_column(:data, on_load: on_load, on_save: on_save)
          expect(on_save).to receive(:call).with(attributes, 'Hello World')
          record = subject.new(attributes)
          subject.save_deferred_columns(record)
        end

        it 'should be called by #save_async' do
          expect(subject).to receive(:save_deferred_columns)
          subject.new(attributes).save_async
        end
      end
    end

    describe '.save_async_deferred_columns' do
      let(:on_load) { on_load_async }
      let(:future) { MockFuture.new('OK') }

      it 'should save the async deferred columns' do
        on_save = double(:proc, call: future)
        subject.async_deferred_column(:data, on_load: on_load, on_save: on_save)
        record = subject.new(attributes)
        expect(subject.save_async_deferred_columns(record).map(&:get)).to include('OK')
      end

      it 'should be resolve by #save_async' do
        future = double(:future)
        record = subject.new(attributes)
        allow(subject).to receive(:save_async_deferred_columns).with(record).and_return([future])
        expect(future).to receive(:get)
        record.save_async
      end
    end

  end
end
