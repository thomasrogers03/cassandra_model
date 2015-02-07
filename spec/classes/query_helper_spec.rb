require 'rspec'

module CassandraModel
  describe QueryHelper do

    class MockQueryHelper
      extend QueryHelper
    end

    subject { MockQueryHelper }

    it_behaves_like 'a query helper'
  end
end