require_relative 'common'

class ImageData < Record
  ImageData.primary_key = [:path, :data]
  ImageData.columns = [:path, :data]
end

Record.config = { hosts: %w(me), keyspace: 'pcrawler' }
ImageData.where(limit: 100, page_size: 10).to_enum(:each).with_index { |row, index| puts "#{index} -> #{row.path}" }
