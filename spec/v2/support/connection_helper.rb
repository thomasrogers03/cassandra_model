module ConnectionHelper
  extend RSpec::Core::SharedContext

  let(:cluster) { Cassandra::Mocks::Cluster.new }
  let(:keyspace_name) { Faker::Lorem.word }
  let!(:keyspace) { cluster.add_keyspace(keyspace_name, false) }
  let(:session) { cluster.connect(keyspace_name) }

  def generate_name
    cleanup_name(Faker::Lorem.sentence)
  end

  def generate_names
    Faker::Lorem.sentences.map { |name| cleanup_name(name) }
  end

  private

  def cleanup_name(name)
    name[0..-2].downcase.gsub(/\s+/, '_')
  end
end
