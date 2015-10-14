require 'rspec'

module CassandraModel
  describe BatchReactor do

    let(:hosts) { [:host1, :host2, :host3] }
    let(:host_buffers) { hosts.map { [] } }
    let(:cluster) { double(:cluster, hosts: hosts) }
    let(:session) { double(:session, keyspace: keyspace) }
    let(:execution_result) { [] }
    let(:keyspace) { 'test' }
    let(:max_batch_size) { 10 }
    let(:batch_klass) { SingleTokenUnloggedBatch }

    subject { BatchReactor.new(cluster, session, batch_klass, max_batch_size: max_batch_size) }

    before do
      allow(cluster).to receive(:find_replicas) do |statement_keyspace, statement|
        host_index = if statement_keyspace == 'test'
                       statement
                     elsif statement_keyspace == 'no_replication'
                       nil
                     else
                       (statement * statement)
                     end
        if host_index
          [hosts[host_index % hosts.count]]
        else
          []
        end
      end
      allow(batch_klass).to receive(:new).and_return(*host_buffers)
      allow(session).to receive(:execute_async) do |batch|
        execution_result << batch
        Cassandra::Future.value(true)
      end
      subject.start.get
    end

    it { is_expected.to be_a_kind_of(::BatchReactor::ReactorCluster) }

    describe '#perform_within_batch' do
      let(:statements) { [0, 1, 2] }

      before do
        unless statements.empty?
          futures = statements.map do |statement|
            subject.perform_within_batch(statement) { |batch| batch << statement }
          end
          Ione::Future.all(futures).get
        end
      end

      it 'should partition the work by keyspace and statement partition key' do
        expect(host_buffers).to match_array([[0], [1], [2]])
      end

      it 'should return a Cassandra::Future' do
        expect(subject.perform_within_batch(0) {}).to be_a_kind_of(Cassandra::Future)
      end

      describe 'batch execution' do
        let(:statements) { [0] }
        let(:hosts) { [:host1] }

        it 'should execute the batch on the provided session' do
          expect(execution_result).to eq([[0]])
        end

        context 'when the batch fails' do
          let(:statements) { [] }
          let(:error) { StandardError.new('Batch blew up!') }

          before do
            allow(session).to(receive(:execute_async)) { |_| Cassandra::Future.error(error) }
          end

          it 'should return a future resolving to a failed result' do
            expect { (subject.perform_within_batch(0) {}.get) }.to raise_error(StandardError, 'Batch blew up!')
          end
        end
      end

      context 'with a different batch klass' do
        let(:batch_klass) { SingleTokenCounterBatch }

        it 'should partition the work by keyspace and statement partition key' do
          expect(host_buffers).to match_array([[0], [1], [2]])
        end
      end

      context 'when no replicas could be found' do
        let(:keyspace) { 'no_replication' }

        it 'should send all work to the first batch' do
          expect(host_buffers).to match_array([[0, 1, 2], [], []])
        end
      end

      context 'with many statements spread across multiple partitions' do
        let(:statements) { (0...6).to_a }

        it 'should partition the work by keyspace and statement partition key' do
          expect(host_buffers).to match_array([[0, 3], [1, 4], [2, 5]])
        end

        context 'with a different keyspace' do
          let(:keyspace) { 'counter_test' }

          it 'should partition the work by keyspace and statement partition key' do
            expect(host_buffers).to match_array([[0, 3], [1, 2, 4, 5], []])
          end
        end
      end

      context 'with different options specified' do
        let(:hosts) { [:host1] }
        let(:host_buffers) { [[], []] }
        let(:max_batch_size) { 2 }

        it 'should forward the options to the unlderying BatchReactor' do
          expect(host_buffers).to match_array([[0, 1], [2]])
        end
      end

    end

  end
end
