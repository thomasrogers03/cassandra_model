module TypeGuessing
  def guess_data_types!
    @guess_data_types = true
  end

  private

  class DataTypeGuess < Struct.new(:column, :counter_type)
    def guessed_type
      postfix_type || :text
    end

    private

    def postfix_type
      if column =~ /_at$/
        :timestamp
      elsif column =~ /_at_id$/
        :timeuuid
      elsif column =~ /_id$/
        :uuid
      elsif column =~ /_(price|average|stddev)$/
        :double
      elsif column =~ /_(total|count)$/
        counter_type
      elsif column =~ /_(year|day|month|index)$/
        :int
      elsif column =~ /_data/
        :blob
      elsif column =~ /_map$/
        'map<string, string>'
      end
    end
  end

  def guessed_data_type(column, counter_type)
    DataTypeGuess.new(column, counter_type).guessed_type
  end

end
