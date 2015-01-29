require 'rspec'

module CassandraModel
  describe Connection do
    class TestConnection
      extend Connection
    end

    it_behaves_like 'a model with a connection', TestConnection

  end
end