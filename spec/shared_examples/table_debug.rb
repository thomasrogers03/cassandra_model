shared_examples_for 'debugging a table' do
  describe '#debug' do
    let(:name) { subject.name }
    let(:internal_table) { subject.send(:table) }
    let(:debug_connection_name) { subject.instance_variable_get(:@connection) }
    let(:debug_connection) { subject.connection }
    let(:debug_partition_key) { subject.partition_key }
    let(:debug_clustering_columns) { subject.clustering_columns }
    let(:debug_primary_key) { subject.primary_key }
    let(:debug_columns) { subject.columns }
    let(:debug) { subject.debug }

    it { expect(debug.name).to eq(name) }
    it { expect(debug.table).to eq(subject.send(:table)) }
    it { expect(debug.connection_name).to eq(debug_connection_name) }
    it { expect(debug.connection).to eq(debug_connection) }
    it { expect(debug.partition_key).to eq(debug_partition_key) }
    it { expect(debug.clustering_columns).to eq(debug_clustering_columns) }
    it { expect(debug.primary_key).to eq(debug_primary_key) }
    it { expect(debug.columns).to eq(debug_columns) }

    it { expect(debug.allows_truncation?).to eq(false) }
    context 'with truncation enabled' do
      before { subject.allow_truncation! }
      it { expect(debug.allows_truncation?).to eq(true) }
    end
  end
end
