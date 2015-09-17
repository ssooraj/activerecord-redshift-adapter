require "spec_helper"

describe ActiveRecord::ConnectionAdapters::RedshiftAdapter do
  before(:all) do
    @connection =  ActiveRecord::Base.redshift_connection(TEST_CONNECTION_HASH)

    @connection.query <<-sql
    CREATE TABLE public.test ( "id" INTEGER NULL, "name" VARCHAR(80) NULL );
    CREATE TABLE public.test2 ( "id" INTEGER, "name" VARCHAR );
    INSERT INTO public.test VALUES (1, 'first');
    INSERT INTO public.test VALUES (2, 'second');
    CREATE SCHEMA test;
    CREATE TABLE test.test ( "id" INTEGER NOT NULL, "is" BOOL NOT NULL );
    CREATE TABLE test.test2 ( "id" INTEGER, "is" BOOL );
    sql
  end

  after(:all) do
    @connection.query <<-sql
    DROP TABLE public.test, public.test2, test.test, test.test2;
    DROP SCHEMA test;
    sql
  end

  describe "#initialize" do
    it "opens a connection" do
      expect(@connection.active?).to be true
    end
  end

  describe "#tables" do
    it "returns all tables in public schema" do
      @connection.schema_search_path = "public"
      expect(@connection.tables).to eq ["public.test", "public.test2"]
    end

    it "returns all tables in all schemas" do
      @connection.schema_search_path = "public, test"
      expect(@connection.tables).to eq(["public.test", "public.test2", "test.test", "test.test2"])
    end
  end

  describe "#columns" do
    it "returns all columns in table in public schema" do
      id = ActiveRecord::ConnectionAdapters::RedshiftColumn.new("id", "", "integer", true)
      name =  ActiveRecord::ConnectionAdapters::RedshiftColumn.new("name", "", "character varying(80)", true)
      expect(@connection.columns("test")).to eq([id, name])
    end

    it "returns all columns in table" do
      id = ActiveRecord::ConnectionAdapters::RedshiftColumn.new("id", "", "integer", false)
      is =  ActiveRecord::ConnectionAdapters::RedshiftColumn.new("is", "", "boolean", false)
      expect(@connection.columns("test.test")).to eq([id, is])
    end
  end

  describe "#table_exists?" do
    it "checks if table in schema exists" do
      expect(@connection.table_exists?("public.test")).to be true
    end

    it "checks if unknown table in schema doesn't exist" do
      expect(@connection.table_exists?("public.null")).to be false
    end

    it "checks if table in implied schema exists" do
      expect(@connection.table_exists?("test2")).to be true
    end
  end

  describe "#current_database" do
    it "returns current database" do
      expect(@connection.current_database).to eq(TEST_CONNECTION_HASH[:database])
    end
  end

  describe "#schema_search_path" do
    it "returns current database" do
      @connection.schema_search_path = '"$user", public'
      expect(@connection.schema_search_path).to eq('"$user", public')
    end
  end

  describe "#update_sql" do
    it "returns the number of updated rows" do
      expect(@connection.update_sql("UPDATE public.test SET name = 'test'")).to eq(2)
    end
  end

  describe "#quote_string" do
    it "quotes the string without surrouding quotes" do
      expect(@connection.quote_string("quote'd")).to eq("quote''d")
    end

    it "returns identical string when no quoting is required" do
      expect(@connection.quote_string("quote")).to eq("quote")
    end
  end
end
