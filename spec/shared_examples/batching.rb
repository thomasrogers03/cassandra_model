module CassandraModel
  shared_examples_for 'a single token batch' do
    describe '#keyspace' do
      its(:keyspace) { is_expected.to be_nil }
    end

    describe '#partition_key' do
      let(:partition_key) { SecureRandom.uuid }
      let(:statement) { double(:statement, partition_key: partition_key) }

      before { subject.statements << statement }

      its(:partition_key) { is_expected.to eq(partition_key) }
    end
  end

  shared_examples_for 'a query running in a batch' do |method, args, statement_args|
    let(:batch_type) { :logged }
    let(:batch_klass) { SingleTokenLoggedBatch }
    let(:batch) { double(:batch) }
    let(:bound_statement) { double(:bound_statement) }

    before do
      allow(statement).to receive(:bind).with(*statement_args).and_return(bound_statement)
      mock_reactor(cluster, batch_klass, {})
      allow(global_reactor).to receive(:perform_within_batch).with(bound_statement).and_yield(batch).and_return(Cassandra::Future.value(['OK']))
      Record.save_in_batch batch_type
    end

    it 'should add the record to the batch' do
      expect(batch).to receive(:add).with(bound_statement)
      Record.new(attributes).public_send(method, *args)
    end

    context 'with a different reactor type' do
      let(:batch_type) { :unlogged }
      let(:batch_klass) { SingleTokenUnloggedBatch }

      it 'should add the record to the batch' do
        expect(batch).to receive(:add).with(bound_statement)
        Record.new(attributes).public_send(method, *args)
      end
    end
  end

end
