# frozen_string_literal: true
require 'aws-sdk-athena'
require 'aws-sdk-s3'
require 'aws-sdk-sns'
require 'csv'
require 'date'
require 'logger'
require 'medusa-client'
require 'objspace'
require 'parquet'
require_relative 'timeout_error'

# Generate Athena tables from Medusa S3 inventory and Medusa DB inventory to find differences
class FindDiff
  MEDUSA_ATHENA_DB = 'medusa_main_inventory'
  MEDUSA_ATHENA_WORKGROUP = 'medusa-inventory-main'
  MEDUSA_INVENTORY_BUCKET = 'medusa-inventories'
  INVENTORY_DIR_PATH = 'main/medusa-main/medusa-main-archived-inventory'
  DIFF_DIR_PATH = 'main/medusa-main/differences'

  attr_reader :athena_client, :s3_client, :medusa_client, :yest_date_str, :yest_date_s3_str, :logger, :sns_client,
              :schema, :parquet_itr, :parquet_bytes, :athena_keys

  def initialize(s3_client: nil, athena_client: nil, sns_client: nil)
    region = 'us-east-2'
    @s3_client = s3_client || Aws::S3::Client.new(region: region)
    @athena_client = athena_client || Aws::Athena::Client.new(region: region)
    @sns_client = sns_client || Aws::SNS::Client.new(region: region)
    @schema = [{ 'key' => 'string' }]
    @parquet_itr = 0
    @parquet_bytes = 0
    @athena_keys = []
    # Configure Medusa client
    Medusa::Client.configuration = {
      medusa_base_url: ENV['MEDUSA_BASE_URL'],
      medusa_user: ENV['MEDUSA_USER'],
      medusa_secret: ENV['MEDUSA_SECRET']
    }
    @medusa_client = Medusa::Client.instance
    yest_date = Date.today - 1
    @yest_date_str = yest_date.strftime('%m_%d')
    @yest_date_s3_str = yest_date.strftime('%Y-%m-%d')
    $stdout.sync = true
    @logger = Logger.new($stdout)
  end

  def run(repositories)
    logger.info("Starting FindDiff for repositories: #{repositories}")
    # Create Athena table for s3 inventory data from the day before
    athena_table_execution_id = create_athena_table
    logger.info("Started Athena table creation with execution ID: #{athena_table_execution_id}")

    # Poll Athena for table creation status
    athena_table_succeeded = athena_execution_succeeded?(athena_table_execution_id)
    exit(1) unless athena_table_succeeded

    # Traverse Medusa repositories, write keys to parquet files, and put parquet files to s3
    error_dirs = traverse_medusa(repositories)

    # write remaining keys to parquet before working on error dirs in case of errors
    organize_parquet(nil, true) unless @athena_keys.empty?

    # large directories that failed to process before require sepcial handling
    logger.info("Error directories: #{error_dirs}") if error_dirs.any?
    process_error_dirs(error_dirs)

    # if athena keys not empty write last parquet file
    organize_parquet(nil, true) unless @athena_keys.empty?

    # Create Athena table for medusa db inventory data (parquet files)
    athena_diff_table_execution_id = create_athena_table_for_diff
    logger.info("Started Athena diff table creation with execution ID: #{athena_diff_table_execution_id}")

    # Poll Athena for diff table creation status
    athena_diff_table_succeeded = athena_execution_succeeded?(athena_diff_table_execution_id)
    exit(1) unless athena_diff_table_succeeded

    # Query Athena for diff between s3 inventory and medusa db inventory
    diff_query_execution_id = query_athena_for_diff
    logger.info("Started Athena diff query with execution ID: #{diff_query_execution_id}")

    # Poll Athena for diff query status
    diff_query_succeeded = athena_execution_succeeded?(diff_query_execution_id)
    exit(1) unless diff_query_succeeded

    # Get Athena query results, put diff csv to s3, and notify via sns
    csv_results = get_athena_query_results(diff_query_execution_id)
    s3_key = put_diff_csv_to_s3(csv_results)
    notify_diff_via_sns(s3_key)
    logger.info('FindDiff completed successfully')

    # Cleanup Athena tables
    drop_athena_table_execution_id = drop_athena_table
    logger.info("Started Athena table drop with execution ID: #{drop_athena_table_execution_id}")
    drop_athena_table_succeeded = athena_execution_succeeded?(drop_athena_table_execution_id)
    drop_athena_diff_table_execution_id = drop_athena_diff_table
    logger.info("Started Athena diff table drop with execution ID: #{drop_athena_diff_table_execution_id}")
    drop_athena_diff_table_succeeded = athena_execution_succeeded?(drop_athena_diff_table_execution_id)
  end

  def athena_execution_succeeded?(execution_id)
    # Poll Athena for query execution status
    logger.info("Processing Athena execution ID: #{execution_id}")
    loop do
      sleep 5
      execution_resp = query_execution(execution_id)
      case query_execution_status(execution_resp)
      when 'SUCCEEDED'
        logger.info('Athena query succeeded')
        return true
      when 'FAILED'
        logger.error("Athena query failed state change reason: #{query_execution_reason(execution_resp)}")
        logger.error("Athena query failed error message: #{query_execution_error(execution_resp)}")
        return false
      else
        logger.info('Athena query in progress...')
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
    execution_resp.query_execution.status.athena_error&.error_message
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
                                                    database: MEDUSA_ATHENA_DB.to_s,
                                                  },
                                                  work_group: MEDUSA_ATHENA_WORKGROUP.to_s,
                                                })
    resp.query_execution_id
  end


  def traverse_medusa(repositories)
    # Traverse Medusa repositories and write keys to parquet files
    logger.info('Traversing Medusa repositories and writing keys to Parquet files')
    error_dirs = []

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
          next unless file_group.storage_level == 'bit_level'

          logger.info("Processing bit level file group: #{file_group.title} (ID: #{file_group.id})")
          root_dir = file_group.directory
          logger.info("Processing directory ID: #{root_dir.id}")

          # And all files and directories contained within.
          begin
            tree_data = get_tree_json(root_dir)
            walk_tree_json(tree_data)
          rescue TimeoutError => e
            logger.info("Timeout while processing directory ID: #{root_dir.id}")
            # Some file groups are too large for Medusa to extract the tree at the root, process these separately
            error_dirs << root_dir.id
          rescue StandardError => e
            logger.error("Unknown error processing directory ID: #{root_dir.id} - #{e.message}")
            exit(1)
          end
        end
      end
    end
    error_dirs
  end

  def get_tree_json(dir)
    url = dir.directory_tree_url
    response = @medusa_client.get(url)

    if response.status < 300
      JSON.parse(response.body)
    elsif response.status == 404
      raise Medusa::NotFoundError, "Directory ID #{@id} not found in Medusa: #{url}"
    elsif response.status == 504
      raise TimeoutError, "Medusa request timed out: #{url}"
    else
      raise IOError, "Received HTTP #{response.status} for GET #{url}"
    end
  end

  def walk_tree_json(dir_struct)
    if dir_struct['subdirectories'].respond_to?(:each)
      dir_struct['subdirectories'].each do |subdir|
        walk_tree_json(subdir)
      end
    end
    if dir_struct['files'].respond_to?(:each)
      dir_struct['files'].each do |file|
        organize_parquet(file['relative_pathname'])
      end
    end
  end

  def organize_parquet(key, force_write = false)
    # max_parquet_bytes = 134_217_728 # 128MB
    max_parquet_bytes = 67_108_864 # 64MB
    if force_write
      write_parquet
      @parquet_itr += 1
      @athena_keys = []
      @parquet_bytes = 0
    else
      key_size = key.bytesize
      if (@parquet_bytes + key_size) >= max_parquet_bytes
      # write parquet file
        write_parquet
        # reset athena keys and parquet bytes
        @parquet_itr += 1
        @athena_keys = [[key]]
        @parquet_bytes = key_size
      else
        @athena_keys << [key]
        @parquet_bytes += key_size
      end
    end
  end

  def write_parquet
    logger.info("Writing Parquet file for #{@yest_date_str} iteration #{@parquet_itr}")
    parquet_file_name = "#{@yest_date_str}_#{@parquet_itr}.parquet"
    Parquet.write_rows(@athena_keys.each, schema: @schema, write_to: parquet_file_name)
    put_parquet_to_s3(parquet_file_name)
  end

  def put_parquet_to_s3(parquet_file_name)
    # Put parquet files to s3
    key_prefix = "#{INVENTORY_DIR_PATH}/#{@yest_date_s3_str}/diff/data"
    logger.info("Putting Parquet file #{parquet_file_name} to s3://#{MEDUSA_INVENTORY_BUCKET}/#{key_prefix}/")
    @s3_client.put_object(bucket: MEDUSA_INVENTORY_BUCKET,
                          key: "#{key_prefix}/#{parquet_file_name}",
                          body: File.read(parquet_file_name))
  end

  def process_error_dirs(error_dirs)
    error_dirs.each do |error_dir|
      logger.info("Processing error directory ID: #{error_dir}")
      # handle error dir files before walking tree
      medusa_error_dir = Medusa::Directory.with_id(error_dir)
      write_dir_files(medusa_error_dir)
      walk_large_json_tree(medusa_error_dir)
    end
  end

  def walk_large_json_tree(parent_dir)
    parent_dir.directories.each do |dir|
      begin
        tree_data = get_tree_json(dir)
        walk_tree_json(tree_data)
      rescue TimeoutError => e
        # handle directory files before moving on to the next child directory
        logger.info("Directory #{dir.id} tree too large, moving on to subdirectories")
        write_dir_files(dir)
        walk_large_json_tree(dir)
      end
    end
  end

  def write_dir_files(dir, retry_count = 0)
    begin
      files = dir.files
      files.each do |file|
        organize_parquet(file.relative_key)
      end
    rescue IOError => e
      logger.error("Error getting files for directory #{dir.id} files: #{e.message}")
      if retry_count < 3
        logger.info("Retrying directory #{dir.id} files")
        write_dir_files(dir, retry_count + 1)
      else
        logger.error("Error getting files for directory #{dir.id} after three tries, exiting: #{e.message}")
        exit(1)
      end
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
                                                    database: MEDUSA_ATHENA_DB.to_s,
                                                  },
                                                  work_group: MEDUSA_ATHENA_WORKGROUP.to_s,
                                                })
    resp.query_execution_id
  end

  def query_athena_for_diff
    # Query athena for diff between s3 inventory and medusa db inventory
    logger.info("Querying Athena for diff between S3 inventory and Medusa DB inventory for date #{@yest_date_str}")
    athena_query = %(SELECT DISTINCT s3.key AS file_in_s3_not_in_medusa
                        FROM #{MEDUSA_ATHENA_DB}."medusa_inventory_#{@yest_date_str}" AS s3
                        LEFT JOIN #{MEDUSA_ATHENA_DB}."medusa_inventory_diff_#{@yest_date_str}" AS med ON s3.key = med.key
                        WHERE med.key IS NULL
                        AND s3.is_delete_marker = false
                        AND s3.is_latest = true;)
    resp = @athena_client.start_query_execution({
                                                  query_string: athena_query,
                                                  query_execution_context: {
                                                    database: MEDUSA_ATHENA_DB.to_s,
                                                  },
                                                  work_group: MEDUSA_ATHENA_WORKGROUP.to_s,
                                                })
    resp.query_execution_id
  end

  def get_athena_query_results(query_id)
    # write results to csv file
    csv_filename = "medusa_diff_#{@yest_date_str}.csv"
    next_token = nil
    loop do
      resp = @athena_client.get_query_results({
                                                query_execution_id: query_id,
                                                next_token: next_token,
                                              })
      resp.result_set.rows.each_with_index do |row, index|
        # skip header row
        next if index.zero?

        key = row.data[0].var_char_value
        # skip "directory" files, these are empty keys to force the creation of a "directory" in s3
        next if key.end_with?('/')

        CSV.open(csv_filename, 'a') do |csv|
          csv << [row.data[0].var_char_value]
        end
      end
      next_token = resp.next_token
      break if next_token.nil?
    end
    csv_filename
  end

  def put_diff_csv_to_s3(csv_filename)
    # Put diff csv file to s3
    key_prefix = "#{DIFF_DIR_PATH}/#{@yest_date_s3_str}"
    s3_key = "#{key_prefix}/#{File.basename(csv_filename)}"
    @s3_client.put_object({
                            bucket: MEDUSA_INVENTORY_BUCKET.to_s,
                            key: s3_key,
                            body: File.read(csv_filename)
                          })
    s3_key
  end

  def notify_diff_via_sns(s3_key)
    # Notify via SNS of diff csv file location
    topic_arn = ENV['SNS_TOPIC_ARN']
    message = "Medusa Diff CSV file available at s3://#{MEDUSA_INVENTORY_BUCKET}/#{s3_key}"
    @sns_client.publish({
                          topic_arn: topic_arn,
                          message: message,
                          subject: 'Medusa Diff CSV File Available'
                        })
  end

  def drop_athena_table
    # Drop athena table
    logger.info('Dropping Athena table')
    drop_athena_table = "DROP TABLE IF EXISTS #{MEDUSA_ATHENA_DB}.medusa_inventory_#{@yest_date_str};"

    resp = @athena_client.start_query_execution({
                                                  query_string: drop_athena_table,
                                                  query_execution_context: {
                                                    database: MEDUSA_ATHENA_DB.to_s,
                                                  },
                                                  work_group: MEDUSA_ATHENA_WORKGROUP.to_s,
                                                })
    resp.query_execution_id
  end

  def drop_athena_diff_table
    # Drop athena diff table
    logger.info('Dropping Athena diff table')
    drop_athena_diff_table = "DROP TABLE IF EXISTS #{MEDUSA_ATHENA_DB}.medusa_inventory_diff_#{@yest_date_str};"

    resp = @athena_client.start_query_execution({
                                                  query_string: drop_athena_diff_table,
                                                  query_execution_context: {
                                                    database: MEDUSA_ATHENA_DB.to_s,
                                                  },
                                                  work_group: MEDUSA_ATHENA_WORKGROUP.to_s,
                                                })
    resp.query_execution_id
  end

end
