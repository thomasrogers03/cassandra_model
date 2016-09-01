require 'integration_spec_helper'

module CassandraModel
  describe 'Basic Record Insertion' do
    TABLE_ATTRIBUTES = {
        partition_key: {path: :text},
        clustering_columns: {tag: :text},
        remaining_columns: {}
    }

    class ImageTagSingle < Record
      TABLE_DEFINITION = TableDefinition.new(TABLE_ATTRIBUTES.merge(name: :tags_single))
      self.table = CassandraModel::MetaTable.new(:single, TABLE_DEFINITION)
    end

    class ImageTagReplicating < Record
      TABLE_DEFINITION = TableDefinition.new(TABLE_ATTRIBUTES.merge(name: :tags_multi))
      self.table = CassandraModel::MetaTable.new(:replicating, TABLE_DEFINITION)
    end

    shared_examples_for 'a simple meta table record insertion' do |klass|
      it 'should be able to insert simple meta table records and then query for them' do
        tags = %w(dog cat mouse bear boar cow)
        tags.map { |tag| klass.create_async(path: '/path/to/image.png', tag: tag) }.map(&:join)

        expected_tags = tags.map { |tag| klass.new(path: '/path/to/image.png', tag: tag) }
        expect(klass.where(path: '/path/to/image.png').get).to match_array(expected_tags)
      end
    end

    context 'in a single node cluster' do
      it_behaves_like 'a simple meta table record insertion', ImageTagSingle
    end

    context 'in a multi node cluster' do
      it_behaves_like 'a simple meta table record insertion', ImageTagReplicating
    end
  end
end
