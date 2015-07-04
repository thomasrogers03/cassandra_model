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

  end
end