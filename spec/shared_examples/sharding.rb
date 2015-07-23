module CassandraModel
  shared_examples_for 'a sharding model' do
    describe '.shard' do
      let(:partition_key) { [:data_set_name, sharding_column] }
      let(:clustering_columns) { [] }
      let(:remaining_columns) { [:meta_data] }
      let(:shard_column) { :meta_data }
      let(:shard_data) { 5 }
      let(:shard_md5) { Digest::MD5.hexdigest(shard_data.to_s) }
      let(:shard_hash) { shard_md5.unpack('L').first }
      let(:shard_proc) { ->(hash) { hash } }
      let(:record) { klass.new(data_set_name: 'data1', shard_column => shard_data).tap(&:save) }

      describe 'sharding with a hashing column' do
        describe 'sharding with a modulus' do
          let(:max_shard) { 3 }

          before { klass.shard(shard_column, max_shard) }

          it 'should assign the result of the sharding column value hash modulo the maximum shard' do
            expect(record.shard).to eq(shard_hash % max_shard)
          end

          context 'with a different shard hashing column' do
            let(:clustering_columns) { [:name] }
            let(:remaining_columns) { [] }
            let(:shard_column) { :name }
            let(:shard_data) { 'hello' }

            it 'should assign the result of the sharding function to the shard column' do
              expect(record.shard).to eq(shard_hash % max_shard)
            end
          end

          context 'with a different maximum' do
            let(:max_shard) { 2 }

            it 'should assign the result of the sharding column value hash modulo the maximum shard' do
              expect(record.shard).to eq(shard_hash % max_shard)
            end
          end

          context 'when the shard is not the last part of the partition key' do
            let(:partition_key) { [:shard, :data_set_name] }

            it 'should still use the last column as the shard' do
              expect(record.data_set_name).to eq(shard_hash % max_shard)
            end
          end
        end

        describe 'sharding with a proc' do
          before { klass.shard(shard_column, &shard_proc) }

          it 'should assign the result of the sharding function to the shard column' do
            expect(record.shard).to eq(shard_hash)
          end

          context 'with a different shard hashing column' do
            let(:clustering_columns) { [:name] }
            let(:remaining_columns) { [] }
            let(:shard_column) { :name }
            let(:shard_data) { 'hello' }

            it 'should assign the result of the sharding function to the shard column' do
              expect(record.shard).to eq(shard_hash)
            end
          end

          context 'with a different sharding function' do
            let(:shard_proc) { ->(hash) { hash % 2 } }

            it 'should assign the result of the sharding function to the shard column' do
              expect(record.shard).to eq(shard_hash % 2)
            end
          end

          context 'when the shard is not the last part of the partition key' do
            let(:partition_key) { [:shard, :data_set_name] }

            it 'should still use the last column as the shard' do
              expect(record.data_set_name).to eq(shard_hash)
            end
          end
        end
      end

      describe 'sharding manually' do
        let(:shard_proc) { ->(instance) { 5 } }

        before { klass.shard(&shard_proc) }

        it 'should assign the result of the sharding function to the shard column' do
          expect(record.shard).to eq(5)
        end

        context 'when the shard function operates on the klass instance' do
          let(:shard_proc) { ->(instance) { meta_data * 5 } }

          it 'should assign the result of the sharding function to the shard column' do
            expect(record.shard).to eq(25)
          end
        end

        context 'when the shard is not the last part of the partition key' do
          let(:partition_key) { [:shard, :data_set_name] }

          it 'should still use the last column as the shard' do
            expect(record.data_set_name).to eq(5)
          end
        end
      end
    end
  end
end