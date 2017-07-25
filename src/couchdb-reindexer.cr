require "./couchdb-reindexer/*"
require "http/client"
require "json"

module Reindexer
  class Indexer
    DEFAULT_OPTIONS = {
      database: "bs",
      server: "127.0.0.1",
      protocol: "http",
      port: 5984
    }

    def initialize
      puts "***************************************************************"
      puts "********************** RUNNING REINDEXER **********************"
      puts "ON: #{default_url}"
      puts "***************************************************************"
      run
    end

    def run
      docs = get_docs
      if docs.to_s != "[{}]"
        docs = docs["rows"]
        docs.each do |doc|
          puts "Document: #{doc["doc"]["_id"]}"
          doc["doc"]["views"].each do |view|
            puts "\tRequesting view: #{doc["doc"]["_id"]}/_view/#{view}"
            response = make_request(view_uri(doc["doc"]["_id"], view))
            if response.status_code == 200
              puts "\tResponse status: OK!"
            else
              puts "\tResponse status: Error!"
            end
          end
        end
      else
        puts "The database is empty or not reachable."
      end
    end

    def default_url
      "#{DEFAULT_OPTIONS[:protocol]}://#{DEFAULT_OPTIONS[:server]}:#{DEFAULT_OPTIONS[:port]}/#{DEFAULT_OPTIONS[:database]}"
    end

    def docs_uri
      URI.parse("#{default_url}/_all_docs?startkey=%22_design/%22&endkey=%22_design0%22&include_docs=true")
    end

    def view_uri(document_name, view_name)
      URI.parse("#{default_url}/#{document_name}/_view/#{view_name}")
    end

    def get_docs
      docs_request = make_request(docs_uri)
      docs_json = "[{}]"
      if docs_request.status_code == 200
        docs_json = docs_request.body
      end
      JSON.parse(docs_json)
    end

    def make_request(url, retries=1)
      HTTP::Client.get url
    end
  end
end
