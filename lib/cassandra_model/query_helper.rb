module CassandraModel
  module QueryHelper

    def self.def_query_helper(name)
      define_method(name) do |*args|
        QueryBuilder.new(self).send(name, *args)
      end
    end

    def_query_helper(:where)
    def_query_helper(:select)
    def_query_helper(:pluck)
    def_query_helper(:paginate)
    def_query_helper(:each_slice)
    def_query_helper(:limit)
    def_query_helper(:order)

    def find_by(attributes)
      where(attributes).first
    end

    def all
      where({})
    end

    def after(record)
      next_cluster(:gt, record)
    end

    def before(record)
      next_cluster(:lt, record)
    end

    private

    def next_cluster(operator, record)
      partition_key = record.partition_key
      clustering_columns = record.clustering_columns
      cluster_comparer = {clustering_columns.keys.public_send(operator) => clustering_columns.values}
      where(partition_key.merge(cluster_comparer))
    end

  end
end
