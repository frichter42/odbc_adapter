module ODBCAdapter
  module Tasks
    class As400OdbcDatabaseTasks

      attr_reader :configuration
      alias_method :config, :configuration

      def initialize(configuration)
        @configuration = configuration
      end

      delegate :connection, :establish_connection, :to => ActiveRecord::Base

      def structure_dump(filename, flags = {})
        establish_connection(config)
        dump = File.open(filename, "w:utf-8")

        schema_name = (connection.schema?.upcase if connection.schema rescue nil)
        schema_name = config[:schema].upcase if config.has_key?(:schema)
        if flags and flags.has_key?(:schema_name)
          schema_name = flags[:schema_name].upcase
        end
        object_types = %w(TYPE VARIABLE SEQUENCE TABLE VIEW ALIAS MASK PERMISSION FUNCTION PROCEDURE TRIGGER CONSTRAINT XSR INDEX)
        object_types.each do |object_type|
          stmt = <<~SQL
            CALL GENERATE_SQL(
              '%', '#{schema_name}',
              '#{object_type}',
              TRIGGER_OPTION => 0,
              CONSTRAINT_OPTION => 0,
              CREATE_OR_REPLACE_OPTION => 1,
              MASK_AND_PERMISSION_OPTION => 0,
              QUALIFIED_NAME_OPTION => 1,
              ADDITIONAL_INDEX_OPTION => 1,
              TEMPORAL_OPTION => 1
            )
          SQL
          res = connection.select_all(stmt)
          res.each do |row|
            dump << row["srcdta"].rstrip + "\n"
          end
        end

        dump.close
      end

    end
  end
end

