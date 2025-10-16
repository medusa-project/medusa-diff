# frozen_string_literal: true
require 'aws-sdk-athena'
require 'aws-sdk-s3'
require 'aws-sdk-sns'
require 'medusa-client'
require 'parquet'
require 'date'
require 'logger'
require 'csv'
# Generate Athena tables from Medusa S3 inventory and Medusa DB inventory to find differences
class FindDiff
    MEDUSA_ATHENA_DB = 'medusa_main_inventory'
    MEDUSA_ATHENA_WORKGROUP = 'medusa-inventory-main'
    MEDUSA_INVENTORY_BUCKET = 'medusa-inventories'
    INVENTORY_DIR_PATH = 'main/medusa-main/medusa-main-archived-inventory'
    DIFF_DIR_PATH = 'main/medusa-main/differences'

    attr_reader :athena_client, :s3_client, :medusa_client, :yest_date_str, :yest_date_s3_str, :logger, :sns_client

    def initialize(s3_client: nil, athena_client: nil, sns_client: nil)
        region = 'us-east-2'
        @s3_client = s3_client || Aws::S3::Client.new(region: region)
        @athena_client = athena_client || Aws::Athena::Client.new(region: region)
        @sns_client = sns_client || Aws::SNS::Client.new(region: region)
        # Configure Medusa client
        Medusa::Client.configuration = {
          medusa_base_url: ENV['MEDUSA_BASE_URL'],
          medusa_user:     ENV['MEDUSA_USER'],
          medusa_secret:   ENV['MEDUSA_SECRET']
        }
        @medusa_client = Medusa::Client.instance
        yest_date = Date.today - 1
        @yest_date_str = yest_date.strftime('%m_%d')
        @yest_date_s3_str = yest_date.strftime('%Y-%m-%d')
        @logger = Logger.new($stdout)
    end

    def run(repositories)
        logger.info("Starting FindDiff for repositories: #{repositories}")
        # Create Athena table for s3 inventory data
        athena_table_execution_id = create_athena_table
        logger.info("Started Athena table creation with execution ID: #{athena_table_execution_id}")

        # Poll Athena for table creation status
        athena_table_succeeded = athena_execution_succeeded(athena_table_execution_id)
        exit(1) unless athena_table_succeeded

        # Traverse Medusa repositories, write keys to parquet files, and put parquet files to s3
        parquet_itr = traverse_medusa_write_parquet(repositories)
        put_parquet_to_s3(parquet_itr)

        # Create Athena table for medusa db inventory data
        athena_diff_table_execution_id = create_athena_table_for_diff
        logger.info("Started Athena diff table creation with execution ID: #{athena_diff_table_execution_id}")

        # Poll Athena for diff table creation status
        athena_diff_table_succeeded = athena_execution_succeeded(athena_diff_table_execution_id)
        exit(1) unless athena_diff_table_succeeded

        # Query Athena for diff between s3 inventory and medusa db inventory
        # diff_query_execution_id = query_athena_for_diff
        diff_query_execution_id = testing_query_athena_for_diff
        logger.info("Started Athena diff query with execution ID: #{diff_query_execution_id}")

        # Poll Athena for diff query status
        diff_query_succeeded = athena_execution_succeeded(diff_query_execution_id)
        exit(1) unless diff_query_succeeded

        # Get Athena query results, put diff csv to s3, and notify via sns
        csv_results = get_athena_query_results(diff_query_execution_id)
        s3_key = put_diff_csv_to_s3(csv_results)
        notify_diff_via_sns(s3_key)
        logger.info("FindDiff completed successfully")

        # Cleanup Athena tables
        drop_athena_table_execution_id = drop_athena_table
        logger.info("Started Athena table drop with execution ID: #{drop_athena_table_execution_id}")
        drop_athena_table_succeeded = athena_execution_succeeded(drop_athena_table_execution_id)
        drop_athena_diff_table_execution_id = drop_athena_diff_table
        logger.info("Started Athena diff table drop with execution ID: #{drop_athena_diff_table_execution_id}")
        drop_athena_diff_table_succeeded = athena_execution_succeeded(drop_athena_diff_table_execution_id)
    end

    def athena_execution_succeeded(execution_id)
        # Poll Athena for query execution status
        logger.info("Processing Athena execution ID: #{execution_id}")
        loop do
            sleep 5
            execution_resp = query_execution(execution_id)
            case query_execution_status(execution_resp)
            when "SUCCEEDED"
                logger.info("Athena query succeeded")
                return true
            when "FAILED"
                logger.error("Athena query failed state change reason: #{query_execution_reason(execution_resp)}")
                logger.error("Athena query failed error message: #{query_execution_error(execution_resp)}")
                return false
            else
                logger.info("Athena query in progress...")
            end
        end
    end

    def query_execution(execution_id)
        @athena_client.get_query_execution({
        query_execution_id: execution_id,
        })
    end

    def query_execution_status(execution_resp)
        execution_resp.query_execution.status.state
    end

    def query_execution_reason(execution_resp)
        execution_resp.query_execution.status.state_change_reason
    end

    def query_execution_error(execution_resp)
        execution_resp.query_execution.status.athena_error.error_message if execution_resp.query_execution.status.athena_error
    end

    def create_athena_table
        # Create athena table from s3 inventory data
        logger.info("Creating Athena table for Medusa inventory data for date #{@yest_date_str}")
        create_query = %Q[CREATE EXTERNAL TABLE IF NOT EXISTS `#{MEDUSA_ATHENA_DB}`.`medusa_inventory_#{@yest_date_str}` (
        `bucket` string,
        `key` string,
        `version_id` string,
        `is_latest` boolean,
        `is_delete_marker` boolean,
        `size` bigint,
        `last_mod_date` bigint
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION 's3://#{MEDUSA_INVENTORY_BUCKET}/#{INVENTORY_DIR_PATH}/#{@yest_date_s3_str}/data/'
        TBLPROPERTIES ('classification' = 'parquet');]

        resp = @athena_client.start_query_execution({
        query_string: create_query,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
        resp.query_execution_id
    end


    def traverse_medusa_write_parquet(repositories)
        # Traverse Medusa repositories and write keys to parquet files
        logger.info("Traversing Medusa repositories and writing keys to Parquet files")
        max_parquet_bytes = 134217728 # 128MB
        parquet_itr = 0
        parquet_bytes = 0
        schema = [{ "key" => "string" }]
        athena_keys = []

        # For each repository...
        repositories.each do |repo|
            client_repo = Medusa::Repository.with_id(repo)
            logger.info("Checking existence of repository: #{repo}")
            logger.info("Repository exists: #{client_repo.exists?}")
            next unless client_repo.exists?
            logger.info("Processing repository: #{client_repo.title} (ID: #{client_repo.id})")
            # For each collection in the repository...
            client_repo.collections.each do |collection|
                logger.info("Processing collection: #{collection.title} (ID: #{collection.id})")
            # And each file group in the collection...
                collection.file_groups.each do |file_group|
                    # If the file group is bit-level, get its root directory...
                    if file_group.storage_level == 'bit_level'
                        logger.info("Processing bit level file group: #{file_group.title} (ID: #{file_group.id})")
                        root_dir = file_group.directory

                        # And all files and directories contained within.
                        root_dir.walk_tree do |node|
                        # For each file, add its relative key to the athena keys array
                            if node.is_a?(Medusa::File)
                                key = node.relative_key
                                key_size = key.bytesize
                                if (parquet_bytes + key_size) < max_parquet_bytes
                                    athena_keys << [key]
                                    parquet_bytes += key_size
                                else
                                #write parquet file
                                    Parquet.write_rows(athena_keys.each, schema: schema, write_to: "#{yest_date_str}_#{parquet_itr}.parquet")
                                    parquet_exists = File.exist?("#{yest_date_str}_#{parquet_itr}.parquet")
                                    logger.info("Wrote Parquet file: #{yest_date_str}_#{parquet_itr}.parquet") if parquet_exists
                                    logger.error("Error writing Parquet file: #{yest_date_str}_#{parquet_itr}.parquet") unless parquet_exists
                                    #reset athena keys and parquet bytes
                                    parquet_itr += 1
                                    athena_keys = [key]
                                    parquet_bytes = key_size
                                end
                            end
                        end
                    end
                end
            end
        end
        # if athena keys not empty write last parquet file
        Parquet.write_rows(athena_keys.each, schema: schema, write_to: "#{yest_date_str}_#{parquet_itr}.parquet") unless athena_keys.empty?
        parquet_exists = File.exist?("#{yest_date_str}_#{parquet_itr}.parquet")
        logger.info("Wrote Parquet file: #{yest_date_str}_#{parquet_itr}.parquet") if parquet_exists
        logger.error("Error writing Parquet file: #{yest_date_str}_#{parquet_itr}.parquet") unless parquet_exists
        parquet_itr
    end


    def put_parquet_to_s3(parquet_itr)
        # Put parquet files to s3
        logger.info("Putting Parquet files to S3")
        key_prefix = "#{INVENTORY_DIR_PATH}/#{@yest_date_s3_str}/diff/data"
        logger.info("Putting Parquet files to s3://#{MEDUSA_INVENTORY_BUCKET}/#{key_prefix}/")
        #for 0 through parquet_itr put parquet file to s3
        (0..parquet_itr).each do |i|
            @s3_client.put_object(bucket: MEDUSA_INVENTORY_BUCKET, key: "#{key_prefix}/#{@yest_date_str}_#{i}.parquet", body: File.read("#{@yest_date_str}_#{i}.parquet"))
        end
    end

    def create_athena_table_for_diff
        # Create athena table from medusa db inventory data
        logger.info("Creating Athena diff table for Medusa inventory diff for date #{@yest_date_str}")
        create_query = %Q[CREATE EXTERNAL TABLE IF NOT EXISTS `#{MEDUSA_ATHENA_DB}`.`medusa_inventory_diff_#{@yest_date_str}` (
        `key` string
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION 's3://#{MEDUSA_INVENTORY_BUCKET}/#{INVENTORY_DIR_PATH}/#{@yest_date_s3_str}/diff/data/'
        TBLPROPERTIES ('classification' = 'parquet');]

        resp = @athena_client.start_query_execution({
        query_string: create_query,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
        resp.query_execution_id
    end

    def query_athena_for_diff
        # Query athena for diff between s3 inventory and medusa db inventory
        logger.info("Querying Athena for diff between S3 inventory and Medusa DB inventory for date #{@yest_date_str}")
        athena_query = %Q{SELECT DISTINCT s3.key AS file_in_s3_not_in_medusa
                        FROM #{MEDUSA_ATHENA_DB}."medusa_inventory_#{@yest_date_str}" AS s3
                        LEFT JOIN #{MEDUSA_ATHENA_DB}."medusa_inventory_diff_#{@yest_date_str}" AS med ON s3.key = med.key
                        WHERE med.key IS NULL;}
        resp = @athena_client.start_query_execution({
        query_string: athena_query,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
        resp.query_execution_id
    end

    def testing_query_athena_for_diff
        # Query athena for diff between s3 inventory and medusa db inventory
        logger.info("Querying Athena for diff between S3 inventory and Medusa DB inventory for date #{@yest_date_str}")
        athena_query = %Q[SELECT DISTINCT s3.key AS file_in_s3_not_in_medusa
                        FROM #{MEDUSA_ATHENA_DB}."medusa_inventory_#{@yest_date_str}" AS s3
                        LEFT JOIN #{MEDUSA_ATHENA_DB}."medusa_inventory_diff_#{@yest_date_str}" AS med ON s3.key = med.key
                        WHERE med.key IS NOT NULL;]
        resp = @athena_client.start_query_execution({
        query_string: athena_query,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
        resp.query_execution_id
    end

    def get_athena_query_results(query_id)
        # write results to csv file
        csv_filename = "medusa_diff_#{@yest_date_str}.csv"
        next_token = nil
        begin
            resp = @athena_client.get_query_results({
            query_execution_id: query_id,
            next_token: next_token,
            })
            resp.result_set.rows.each_with_index do |row, index|
                # skip header row
                next if index == 0
                CSV.open(csv_filename, "a") do |csv|
                    csv << [row.data[0].var_char_value]
                end
            end
            next_token = resp.next_token
        end while !next_token.nil?
        csv_filename
    end

    def put_diff_csv_to_s3(csv_filename)
        # Put diff csv file to s3
        key_prefix = "#{DIFF_DIR_PATH}/#{@yest_date_s3_str}"
        s3_key = "#{key_prefix}/#{File.basename(csv_filename)}"
        @s3_client.put_object({
            bucket: "#{MEDUSA_INVENTORY_BUCKET}",
            key: s3_key,
            body: File.read(csv_filename)
        })
        s3_key
    end

    def notify_diff_via_sns(s3_key)
        # Notify via SNS of diff csv file location
        topic_arn = ENV["SNS_TOPIC_ARN"]
        message = "Medusa Diff CSV file available at s3://#{MEDUSA_INVENTORY_BUCKET}/#{s3_key}"
        @sns_client.publish({
            topic_arn: topic_arn,
            message: message,
            subject: "Medusa Diff CSV File Available"
        })
    end

    def drop_athena_table
        # Drop athena table
        logger.info("Dropping Athena table")
        drop_athena_table = "DROP TABLE IF EXISTS #{MEDUSA_ATHENA_DB}.medusa_inventory_#{@yest_date_str};"

        @athena_client.start_query_execution({
        query_string: drop_athena_table,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
    end

    def drop_athena_diff_table
        # Drop athena diff table
        logger.info("Dropping Athena diff table")
        drop_athena_diff_table = "DROP TABLE IF EXISTS #{MEDUSA_ATHENA_DB}.medusa_inventory_diff_#{@yest_date_str};"

        @athena_client.start_query_execution({
        query_string: drop_athena_diff_table,
        query_execution_context: {
            database: "#{MEDUSA_ATHENA_DB}",
        },
        work_group: "#{MEDUSA_ATHENA_WORKGROUP}",
        })
    end

end
