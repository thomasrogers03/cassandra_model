require_relative 'common'

class Painting < CassandraModel::Record
  extend CassandraModel::DataModelling

  model_data do |inquirer, data_set|
    inquirer.knows_about(:artist, :title, :year)
    inquirer.knows_about(:artist)
    inquirer.knows_about(:title)
    inquirer.knows_about(:year)
    inquirer.defaults(:year).to(0)

    data_set.is_defined_by(:price, :artist, :title, :year)
    data_set.knows_about(:comments)
    data_set.change_type_of(:price).to(:double)
    data_set.change_type_of(:year).to(:int)
  end
  table.allow_truncation!

  def self.prepare_example
    table.truncate!
    futures = []
    futures << create_async(artist: 'Henry', title: 'Pretty Josephine', year: 1899, price: 1_200_000.00, comments: 'Damaged')
    futures << create_async(artist: 'Henry', title: 'Dark Corridor', year: 1893, price: 8_000.00, comments: 'Corner ripped')
    futures << create_async(artist: 'Timothy', title: 'Bright Atrium', year: 1899, price: 14_000.00, comments: 'Perfect condition')
    futures.map(&:join)
  end
end

Painting.prepare_example

query = Painting.where(artist: 'Henry')
paintings = query.map { |row| row.attributes.except(:artist) } * "\n"
puts "=> paintings by Henry:\n#{paintings}\n\n"

query = Painting.where(title: 'Bright Atrium')
paintings = query.map { |row| row.attributes.except(:artist) } * "\n"
puts "=> paintings with title 'Bright Atrium':\n#{paintings}\n\n"

query = Painting.where(year: 1899)
paintings = query.map { |row| row.attributes.except(:year) } * "\n"
puts "=> paintings from the year 1899:\n#{paintings}\n\n"

