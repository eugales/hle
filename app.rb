require 'mysql2'
require 'dotenv/load'

class Correction
    def initialize(val)
        @name = val
        @parts = []
    end

    def perform
        change_abbrs
        delete_dot
        split_by_slash
        add_parentheses
        rotate_by_array_length
        normalize
        @name
    end

    private

    def change_abbrs
        abbrs = {
            "Twp" => "Township",
            "Hwy" => "Highway",
            "CCH" => "Country Club Hills"
        }
        abbrs.each { |k,v| @name.gsub!(k,v)}
    end

    def delete_dot
        @name.delete!('.')
    end

    def split_by_slash
        @parts = @name.split('/').map(&:strip)
    end

    def add_parentheses
        @parts = @parts.map do |p|
            if p.include? ?,
                comma_parts = p.split(',').map(&:strip)
                comma_parts.first.downcase!
                comma_parts[comma_parts.length-1] = comma_parts.last.prepend('(') << ')'
                p = comma_parts.join(' ')
            end
            p
        end
    end

    def rotate_by_array_length
       for _ in 1..@parts.length - 1 do
           @parts = @parts.rotate(1)
       end
    end

    def normalize
        @parts.map { |p| p.downcase! } if @parts.length == 1
        @parts.drop(1).map { |p| p.downcase! } if @parts.length > 1 && !@parts.join(' ').match(/[()]/)
        @parts.insert(-2, 'and') if @parts.length > 2
        @name = @parts.join(' ')
        max = @name.scan(/\S+/).length
        1.upto(max).each_with_object(@name) do |n, s| 
            s.gsub!(/((?:\b\s*[A-z]+){#{n}})\1/i, '\1')
        end
    end
end

update_query = ""

# Needs .env file
client = Mysql2::Client.new(
    host: ENV['HOST'], 
    database: ENV['DATABASE'], 
    username: ENV['USERNAME'], 
    password: ENV['PASSWORD'], 
)
client.query("delete from hle_dev_test_adil_mamyrkhanov where candidate_office_name = '';")
table = client.query("select id, candidate_office_name from hle_dev_test_adil_mamyrkhanov a;", :cast => false)

table.each_with_index do |row, i|
    id = row['id']
    name = row['candidate_office_name']

    clean_name = Correction.new(name).perform
    sentence = "The candidate is running for the #{clean_name} office"
    update_query += "update hle_dev_test_adil_mamyrkhanov set clean_name=\"%s\", sentence=\"%s\" where id = %s;\n" % [clean_name, sentence, id.to_s]
end

updating_client = Mysql2::Client.new(
    host: ENV['HOST'], 
    database: ENV['DATABASE'], 
    username: ENV['USERNAME'], 
    password: ENV['PASSWORD'], 
    flags: Mysql2::Client::MULTI_STATEMENTS
)
puts 'Update statements sent to MySQL, wait...'
updating_client.query(update_query)
while updating_client.next_result
  result = updating_client.store_result
  puts "Result: #{result}" 
end
updating_client.close
update_query = ''




