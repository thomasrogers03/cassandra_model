require_relative 'common'

class WorkData < CassandraModel::Record
  extend CassandraModel::DataModelling

  Data = Struct.new(:data)

  deferred_column :data,
                  on_load: ->(attributes) { Marshal.load(attributes[:serialized_data]) if attributes[:serialized_data] },
                  on_save: ->(attributes, value) { attributes[:serialized_data] = Marshal.dump(value) }

  model_data do |inquirer, data_set|
    inquirer.knows_about(:work_id, :work_type)
    inquirer.knows_about(:work_type)
    inquirer.knows_about(:ran_at)
    inquirer.defaults(:ran_at).to(Time.at(0))

    data_set.is_defined_by(:ran_at, :inserted_at, :work_id, :work_type)
    data_set.change_type_of(:ran_at).to(:timestamp)
    data_set.change_type_of(:inserted_at).to(:timestamp)
    data_set.knows_about(:serialized_data)
    data_set.rotates_storage_across(3).tables_every(1.week)
  end
  table.allow_truncation!

  class << self

    def prepare_example
      table.truncate!
      futures = create_work_data('Fake Work', 'Good Work', Time.at(0))
      futures += 10.times.map { create_work_data(SecureRandom.uuid, 'Hard Work', Time.at(0)) }.flatten
      futures.map(&:get)
    end

    private

    def create_work_data(work_id, work_type, ran_at)
      10.times.map do
        new(work_id: work_id, work_type: work_type, ran_at: ran_at, inserted_at: Time.now).tap do |record|
          record.data = Data.new(data: [SecureRandom.uuid])
        end.save_async
      end
    end

  end
end

puts "=> writing to table #{WorkData.table.name} this week"
WorkData.prepare_example

data = WorkData.where({}).limit(3).map(&:data).map(&:data) * "\n"
puts "=> sample data:\n#{data}\n\n"

data_count = WorkData.where(work_type: 'Hard Work').get.count
puts "=> #{data_count} items for Hard Work"

data_count = WorkData.where(work_type: 'Good Work').get.count
puts "=> #{data_count} items for Good Work"

data_count = WorkData.where(ran_at: Time.at(0)).get.count
puts "=> #{data_count} items ran at #{Time.at(0)}"

data_count = WorkData.where(work_id: 'Fake Work', work_type: 'Good Work').get.count
puts "=> #{data_count} items for Fake Work id"
