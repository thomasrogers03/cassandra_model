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

    context 'when the batch has been executed' do
      let(:results) { [] }
      let(:result_page) { MockPage.new(true, nil, results) }

      before { subject.result = result_page }

      describe '#execution_info' do
        its(:execution_info) { is_expected.to eq(result_page.execution_info) }
      end

      describe '#empty?' do
        its(:empty?) { is_expected.to eq(result_page.empty?) }

        context 'with some results' do
          let(:results) { ['[applied]' => false] }

          its(:empty?) { is_expected.to eq(result_page.empty?) }
        end
      end

      describe 'enumerability' do
        it 'should delegate to the underlying result' do
          expect(subject.first).to be_nil
        end

        context 'with some results' do
          let(:results) { ['[applied]' => false] }

          it 'should delegate to the underlying result' do
            expect(subject.first['[applied]']).to eq(false)
          end
        end
      end
    end
  end

  shared_examples_for 'a query running in a batch' do |method, args, statement_args|
    let(:batch_type) { :logged }
    let(:batch_klass) { SingleTokenLoggedBatch }
    let(:query_result) { MockPage.new(true, nil, []) }
    let(:batch) { double(:batch, first: query_result.first, execution_info: query_result.execution_info) }
    let(:bound_statement) { double(:bound_statement) }

    before do
      allow(statement).to receive(:bind).with(*statement_args).and_return(bound_statement)
      mock_reactor(cluster, batch_klass, {})
      allow(global_reactor).to receive(:perform_within_batch).with(bound_statement) do |&block|
        result = block.call(batch)
        Cassandra::Future.value(result)
      end
      subject.save_in_batch batch_type
    end

    it 'should return a ThomasUtils::Observation' do
      allow(batch).to receive(:add).and_return(batch)
      expect(subject.new(attributes).public_send(method, *args)).to be_a_kind_of(ThomasUtils::Observation)
    end

    it 'should add the record to the batch' do
      expect(batch).to receive(:add).with(bound_statement).and_return(batch)
      subject.new(attributes).public_send(method, *args).get
    end

    context 'with a different reactor type' do
      let(:batch_type) { :unlogged }
      let(:batch_klass) { SingleTokenUnloggedBatch }

      it 'should add the record to the batch' do
        expect(batch).to receive(:add).with(bound_statement).and_return(batch)
        subject.new(attributes).public_send(method, *args).get
      end
    end
  end

end
