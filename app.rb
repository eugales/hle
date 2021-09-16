require 'mysql2/em'
require 'dotenv/load'
require 'eventmachine'
require 'pry'

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

def partition(size)
    case size
    when 1..500
        1
    when 501..1000
        2
    when 1001..1500
        3
    when 1501..2000
        4
    when 2001..2500
        4
    when 2501..3000
        5
    when 3001..3500
        6
    when 3500..4000
        7
    else
        0
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
count_result = client.query("select count(id) from hle_dev_test_adil_mamyrkhanov a;")
table_count = count_result.first['count(id)']
delim = partition(table_count)
limit = (table_count / delim).ceil
table = client.query("select id, candidate_office_name from hle_dev_test_adil_mamyrkhanov a;", :cast => false)
EM.run do
    table.each_with_index do |row, i|
        id = row['id']
        name = row['candidate_office_name']

        clean_name = Correction.new(name).perform
        sentence = "The candidate is running for the #{clean_name} office"
        update_query += "update hle_dev_test_adil_mamyrkhanov set clean_name=\"%s\", sentence=\"%s\" where id = %s;\n" % [clean_name, sentence, id.to_s]

        # update db with a part of all update statements. The statements splited according to total / partition
        if i % limit > limit - 2
            updating_client = Mysql2::EM::Client.new(
                host: ENV['HOST'], 
                database: ENV['DATABASE'], 
                username: ENV['USERNAME'], 
                password: ENV['PASSWORD'], 
                flags: Mysql2::Client::MULTI_STATEMENTS
            )
            defer = updating_client.query(update_query, :async => true)
            defer.callback do |result|
                puts "Result of #{i} statements: #{result.to_a.inspect}"
                updating_client.close
            end
            update_query = ''
        end
    end


    unless update_query.empty?
        updating_client = Mysql2::EM::Client.new(
            host: ENV['HOST'], 
            database: ENV['DATABASE'], 
            username: ENV['USERNAME'], 
            password: ENV['PASSWORD'], 
            flags: Mysql2::Client::MULTI_STATEMENTS
        )
        defer = updating_client.query(update_query, :async => true)
        defer.callback do |result|
            puts "Last statements: #{result.to_a.inspect}"
            updating_client.close
        end
        update_query = ''
        EventMachine::stop_event_loop
    else
        EventMachine::stop_event_loop
    end
end
puts "Done!"




