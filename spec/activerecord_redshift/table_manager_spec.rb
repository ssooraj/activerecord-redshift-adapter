require "spec_helper"

describe ActiverecordRedshift::TableManager do
  TEST_SCHEMA = "test"
  TEST_MANAGER_EXEMPLAR_TABLE = "test.test"
  TEST_MANAGER_TABLE = "test.test2"

  before(:all) do
    @connection =  ActiveRecord::Base.redshift_connection(TEST_CONNECTION_HASH)

    @connection.query <<-sql
      CREATE SCHEMA #{TEST_SCHEMA};
    sql

    @connection.query <<-sql
      CREATE TABLE #{TEST_MANAGER_EXEMPLAR_TABLE} (
        id INTEGER NOT NULL,
        isa BOOL NOT NULL
      );
    sql
  end

  after(:all) do
    @connection.query "DROP TABLE #{TEST_MANAGER_EXEMPLAR_TABLE};"
    @connection.query "DROP SCHEMA #{TEST_SCHEMA};"
  end

  it "#duplicate_table_sql returns sql to duplicate the table" do
    begin
      tm = ActiverecordRedshift::TableManager.new(@connection, table_name: TEST_MANAGER_TABLE)
      sql = tm.duplicate_table_sql(exemplar_table_name: TEST_MANAGER_EXEMPLAR_TABLE)
      sql.gsub!(/\s+/m," ") # normalize whitespace
      expect(sql).to eq(
        " create temporary table test.test2 ( id integer not null, isa boolean not null ) diststyle even "
      )
    rescue ActiveRecord::StatementInvalid => e
      pending "This spec will fail when run against a PostgreSQL database, instead of Redshift."
      fail
    end
  end
end
