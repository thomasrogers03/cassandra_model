require 'rspec'

module CassandraModel
  describe CompositeRecordStatic do
    class MockRecord < CassandraModel::Record
      extend CompositeRecordStatic
    end

    let(:columns) { [] }

    before do
      MockRecord.reset!
      MockRecord.columns = columns
    end

    describe '.columns' do
      let(:columns) { [:rk_model, :series, :ck_model, :meta_data] }

      it 'should reduce the columns starting with rk_ or ck_ to base columns' do
        expect(MockRecord.columns).to eq([:model, :series, :meta_data])
      end

      it 'should create methods for the reduced columns, rather than the internal ones' do
        record = MockRecord.new({})
        record.model = 'KKBBCD'
        expect(record.model).to eq('KKBBCD')
      end

      context 'with a different set of columns' do
        let(:columns) { [:rk_model, :rk_series, :rk_colour, :ck_price, :ck_model, :ck_colour, :meta_data] }

        it 'should reduce the columns starting with rk_ or ck_ to base columns' do
          expect(MockRecord.columns).to eq([:model, :series, :colour, :price, :meta_data])
        end
      end
    end

    shared_examples_for 'a composite column map' do |method, prefix|
      describe ".#{method}" do
        let(:columns) { [:rk_model, :rk_series, :rk_colour, :ck_price, :ck_model, :ck_colour, :meta_data] }

        before { MockRecord.columns }

        {"#{prefix}_model".to_sym => :model, "#{prefix}_colour".to_sym => :colour}.each do |actual, composite|
          it 'should map a reduced row key to its original name' do
            expect(MockRecord.public_send(method)[composite]).to eq(actual)
          end

          it 'should map a the original row key to its reduced name' do
            expect(MockRecord.public_send(method)[actual]).to eq(composite)
          end
        end
      end
    end

    it_behaves_like 'a composite column map', :composite_pk_map, :rk
    it_behaves_like 'a composite column map', :composite_ck_map, :ck

  end
end