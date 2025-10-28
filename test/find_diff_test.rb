# frozen_string_literal: true

require 'test_helper'

class FindDiffTest < Minitest::Test
  def setup
    @s3_client = Aws::S3::Client.new(stub_responses: true)
    @athena_client = Aws::Athena::Client.new(stub_responses: true)
    @sns_client = Aws::SNS::Client.new(stub_responses: true)
    @find_diff = FindDiff.new(s3_client: @s3_client, athena_client: @athena_client, sns_client: @sns_client)
  end

  def test_run
    query_resp = Aws::Athena::Types::StartQueryExecutionOutput.new(query_execution_id: '1234')
    execution_resp = Aws::Athena::Types::GetQueryExecutionOutput.new(query_execution: Aws::Athena::Types::QueryExecution.new(status: Aws::Athena::Types::QueryExecutionStatus.new(state: 'SUCCEEDED')))
    results_resp = Aws::Athena::Types::GetQueryResultsOutput.new(result_set: Aws::Athena::Types::ResultSet.new(rows: [Aws::Athena::Types::Row.new(
        data: [Aws::Athena::Types::Datum.new(var_char_value: 'file_in_s3_not_in_medusa')]), Aws::Athena::Types::Row.new(data: [Aws::Athena::Types::Datum.new(var_char_value: 'testKey')])]))

    @athena_client.expects(:start_query_execution).times(5).returns(query_resp)
    @athena_client.expects(:get_query_execution).times(5).returns(execution_resp)
    @athena_client.expects(:get_query_results).once.returns(results_resp)
    @s3_client.expects(:put_object).times(2).returns(true)
    @sns_client.expects(:publish).once.returns(true)

    @find_diff.run([1])

    File.delete("medusa_diff_#{@find_diff.yest_date_str}.csv") if File.exist?("medusa_diff_#{@find_diff.yest_date_str}.csv")
  end

  def test_athena_execution_succeeded
    execution_resp = Aws::Athena::Types::GetQueryExecutionOutput.new(query_execution: Aws::Athena::Types::QueryExecution.new(status: Aws::Athena::Types::QueryExecutionStatus.new(state: 'FAILED', state_change_reason: 'Test failure reason', athena_error: Aws::Athena::Types::AthenaError.new(error_message: 'Test error message'))))

    @athena_client.expects(:get_query_execution).with({query_execution_id: '1234'}).returns(execution_resp)

    succeeded = @find_diff.athena_execution_succeeded?('1234')
    assert_equal false, succeeded
  end

  def test_create_athena_table
    mock_resp = Minitest::Mock.new
    mock_resp.expect :query_execution_id, '1234'
    @athena_client.expects(:start_query_execution).with do |params|
      params[:query_string].include?('CREATE EXTERNAL TABLE IF NOT EXISTS')
    end.returns(mock_resp)

    @find_diff.create_athena_table
  end

  def test_traverse_medusa_organize_parquet_write_and_put_to_s3
    error_dirs = @find_diff.traverse_medusa([1])
    parquet_itr = @find_diff.parquet_itr
    assert error_dirs.empty?
    assert parquet_itr.is_a?(Integer)

    @find_diff.write_parquet
    assert File.exist?("#{@find_diff.yest_date_str}_0.parquet")

    @s3_client.expects(:put_object).once
    @find_diff.put_parquet_to_s3
  end


  def test_process_error_dirs
    @find_diff.expects(:organize_parquet).times(4)

    @find_diff.process_error_dirs([2173594262])
  end

  def test_create_athena_table_for_diff
    mock_resp = Minitest::Mock.new
    mock_resp.expect :query_execution_id, '1234'
    @athena_client.expects(:start_query_execution).with do |params|
      params[:query_string].include?("medusa_inventory_diff_#{@find_diff.yest_date_str}")
    end.returns(mock_resp)

    @find_diff.create_athena_table_for_diff
  end

  def test_query_athena_for_diff
    mock_resp = Minitest::Mock.new
    mock_resp.expect :query_execution_id, '1234'
    @athena_client.expects(:start_query_execution).with do |params|
      params[:query_string].include?(%Q{LEFT JOIN #{FindDiff::MEDUSA_ATHENA_DB}."medusa_inventory_diff_#{@find_diff.yest_date_str}" AS med ON s3.key = med.key}) && params[:query_string].include?('WHERE med.key IS NULL;')
    end.returns(mock_resp)

    @find_diff.query_athena_for_diff
  end

  def test_get_athena_query_results
    test_csv = "medusa_diff_#{@find_diff.yest_date_str}.csv"
    results_resp = Aws::Athena::Types::GetQueryResultsOutput.new(result_set: Aws::Athena::Types::ResultSet.new(rows: [Aws::Athena::Types::Row.new(
        data: [Aws::Athena::Types::Datum.new(var_char_value: 'file_in_s3_not_in_medusa')]), Aws::Athena::Types::Row.new(data: [Aws::Athena::Types::Datum.new(var_char_value: 'testKey')])]))
    @athena_client.expects(:get_query_results).with({query_execution_id: '1234', next_token: nil}).returns(results_resp)

    csv_name = @find_diff.get_athena_query_results('1234')
    data = CSV.read(csv_name)
    assert_equal test_csv, csv_name
    assert_equal 1, data.length
    assert_equal 'testKey', data[0][0]

    File.delete("medusa_diff_#{@find_diff.yest_date_str}.csv") if File.exist?("medusa_diff_#{@find_diff.yest_date_str}.csv")
  end

  def test_put_diff_csv_to_s3
    @s3_client.expects(:put_object).once.returns(true)

    @find_diff.put_diff_csv_to_s3('test/test.csv')
  end

  def test_notify_diff_via_sns
    @sns_client.expects(:publish).once.returns(true)

    @find_diff.notify_diff_via_sns('testS3Key')
  end

  def test_drop_athena_table
    mock_resp = Minitest::Mock.new
    mock_resp.expect :query_execution_id, '1234'
    @athena_client.expects(:start_query_execution).with do |params|
      params[:query_string].include?("DROP TABLE IF EXISTS #{FindDiff::MEDUSA_ATHENA_DB}.medusa_inventory_#{@find_diff.yest_date_str}")
    end.returns(mock_resp)

    @find_diff.drop_athena_table
  end

  def test_drop_athena_diff_table
    mock_resp = Minitest::Mock.new
    mock_resp.expect :query_execution_id, '1234'
    @athena_client.expects(:start_query_execution).with do |params|
      params[:query_string].include?("DROP TABLE IF EXISTS #{FindDiff::MEDUSA_ATHENA_DB}.medusa_inventory_diff_#{@find_diff.yest_date_str}")
    end.returns(mock_resp)

    @find_diff.drop_athena_diff_table
  end

end
