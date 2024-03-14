module ODBCAdapter
  class Column < ActiveRecord::ConnectionAdapters::Column
    attr_reader :native_type

    # Add the native_type accessor to allow the native DBMS to report back what
    # it uses to represent the column internally.
    # rubocop:disable Metrics/ParameterLists
    def initialize(name, default, sql_type_metadata = nil, null = true, table_name = nil, native_type = nil, default_function = nil, collation = nil)
      super(name, default, sql_type_metadata, null, table_name)
      @native_type = native_type
    end
  end

  module DatabaseStatements
    # we override some methods in odbc_adapter/database_statements
    # to work around issues on as400

    # Begins the transaction (and turns off auto-committing).
    def begin_db_transaction
      execute("set transaction isolation level read committed")
    end

    # Commits the transaction (and turns on auto-committing).
    def commit_db_transaction
      execute("commit")
      execute("set transaction isolation level no commit")
    end

    # Rolls back the transaction (and turns on auto-committing). Must be
    # done if the transaction block raises an exception or returns false.
    def exec_rollback_db_transaction
      execute("rollback")
      execute("set transaction isolation level no commit")
    end
  end

  module Adapters
    # A default adapter used for databases that are no explicitly listed in the
    # registry. This allows for minimal support for DBMSs for which we don't
    # have an explicit adapter.
    class As400ODBCAdapter < ActiveRecord::ConnectionAdapters::ODBCAdapter

      PRIMARY_KEY = "INTEGER GENERATED AlWAYS AS IDENTITY"

      # Using a BindVisitor so that the SQL string gets substituted before it is
      # sent to the DBMS (to attempt to get as much coverage as possible for
      # DBMSs we don't support).
      def arel_visitor
        Arel::Visitors::ODBC_AS400.new(self)
      end

      # Explicitly turning off prepared_statements in the as400 adapter because
      # DB2/400 does only understand "where foo = ?", not "where foo = $1"
      # can we do anything about this?
      def prepared_statements
        false
      end

      # Turning off support for migrations because there is no information to
      # go off of for what syntax the DBMS will expect.
      def supports_migrations?
        true
      end

      def quote_column_name(name)
        name.to_s
      end

      # type cast values from DB
      # we cast decimal and numeric to BigDecimal, when having a scale > 0 or a size that does not fit in an int
      def dbms_type_cast(column, values)
        # Rails.logger.debug("dbms_type_cast called with columns: #{columns.inspect} and values: #{values.inspect}")
        values.each do |row|
          row.each_index do |idx|
            if [ODBC::SQL_DECIMAL, ODBC::SQL_NUMERIC].include?(column[idx].type)
              if column[idx].scale == 0 and column[idx].precision < 10
                row[idx] = row[idx].to_i
              else
                row[idx] = row[idx].to_d
              end
            end
          end
        end
      end

      # see sysibm.sqltypeinfo
      def type_odbc_to_ruby
        {
          ODBC::SQL_CHAR => :string,
          -8 => :string,  # NVARCHAR
          ODBC::SQL_VARCHAR => :string,
          -9 => :string,  # NVARCHAR
          ODBC::SQL_DATE => :date,
          ODBC::SQL_TYPE_DATE => :date,
          ODBC::SQL_TIME => :time,
          ODBC::SQL_TYPE_TIME => :time,
          ODBC::SQL_DATETIME => :datetime,
          ODBC::SQL_TIMESTAMP => :datetime,
          ODBC::SQL_TYPE_TIMESTAMP => :datetime,
          ODBC::SQL_DECIMAL => :decimal,
          ODBC::SQL_NUMERIC => :decimal,
          ODBC::SQL_INTEGER => :integer,
          ODBC::SQL_BIGINT => :decimal,
          ODBC::SQL_SMALLINT => :integer,
          ODBC::SQL_FLOAT => :float,
          ODBC::SQL_REAL => :float,
          ODBC::SQL_DOUBLE => :float,
          ODBC::SQL_LONGVARBINARY => :binary,
          ODBC::SQL_VARBINARY => :binary,
          ODBC::SQL_BINARY => :binary,
          -10 => :string,  # NCLOB
          -1 => :string,   # CLOB
        }
      end

      # override columns initializer
      def columns(table_name, _name = nil)
        Rails.logger.debug("column initializer in As400OdbcAdapter")
        table_name_native = native_case(table_name.to_s)
        stmt   = @connection.columns(table_name_native)
        result = stmt.fetch_all || []
        stmt.drop

        result.each_with_object([]) do |col, cols|
          col_name        = col[3]  # SQLColumns: COLUMN_NAME
          col_default     = col[12] # SQLColumns: COLUMN_DEF
          col_sql_type    = col[4]  # SQLColumns: DATA_TYPE
          col_native_type = col[5]  # SQLColumns: TYPE_NAME
          col_limit       = col[6]  # SQLColumns: COLUMN_SIZE
          col_scale       = col[8]  # SQLColumns: DECIMAL_DIGITS

          # SQLColumns: IS_NULLABLE, SQLColumns: NULLABLE
          col_nullable = nullability(col_name, col[17], col[10])

          sql_type_str = col_native_type
          if col_limit and col_limit > 0 and not %w(DATE TIME).include?(col_native_type)
            sql_type_str = "#{col_native_type}(#{col_limit})"
          end
          if col_limit and col_limit > 0 and col_scale and col_scale > 0
            sql_type_str = "#{col_native_type}(#{col_limit},#{col_scale})"
          end

          args = { sql_type: sql_type_str, type: col_sql_type, limit: col_limit }
#          args = { sql_type: col_sql_type, type: col_sql_type, limit: col_limit }

          if type_odbc_to_ruby[col_sql_type].nil?
            Rails.logger.debug "No ruby type for odbc type #{col_sql_type} found. Native Type is '#{col_native_type}'"
          end
          args[:type] = (type_odbc_to_ruby[col_sql_type] || col_sql_type)

          if col_native_type == self.class::BOOLEAN_TYPE
            args[:sql_type] = 'boolean'
            args[:type] = :boolean
          end

          if [ODBC::SQL_DECIMAL, ODBC::SQL_NUMERIC].include?(col_sql_type)
            args[:scale]     = col_scale || 0
            args[:precision] = col_limit
#            args[:sql_type] = "#{col_native_type}(#{col_limit},#{col_scale || 0})"
            if args[:scale] == 0 and col_limit < 10
              args[:type] = :integer
            end
          end
          sql_type_metadata = ActiveRecord::ConnectionAdapters::SqlTypeMetadata.new(**args)

          cols << new_column(format_case(col_name), col_default, sql_type_metadata, col_nullable, table_name, col_native_type)
        end
      end

      def initialize_type_map(m = type_map) # :nodoc:
#        puts_log "initialize_type_map"
        register_class_with_limit m, %r(boolean)i,   ActiveRecord::Type::Boolean
        register_class_with_limit m, %r(char)i,      ActiveRecord::Type::String
        register_class_with_limit m, %r(binary)i,    ActiveRecord::Type::Binary
        register_class_with_limit m, %r(text)i,      ActiveRecord::Type::Text
        register_class_with_precision m, %r(date)i,      ActiveRecord::Type::Date
        register_class_with_precision m, %r(time)i,      ActiveRecord::Type::Time
        register_class_with_precision m, %r(datetime)i,  ActiveRecord::Type::DateTime
        register_class_with_limit m, %r(float)i,     ActiveRecord::Type::Float

        m.register_type %r(^bigint)i,    ActiveRecord::Type::Integer.new(limit: 8)
        m.register_type %r(^int)i,       ActiveRecord::Type::Integer.new(limit: 4)
        m.register_type %r(^smallint)i,  ActiveRecord::Type::Integer.new(limit: 2)
        m.register_type %r(^tinyint)i,   ActiveRecord::Type::Integer.new(limit: 1)
		
        m.alias_type %r(blob)i,      'binary'
        m.alias_type %r(clob)i,      'text'
        m.alias_type %r(timestamp)i, 'datetime'
        m.alias_type %r(numeric)i,   'decimal'
        m.alias_type %r(number)i,    'decimal'
        m.alias_type %r(double)i,    'float'
				
        m.register_type(%r(decimal)i) do |sql_type|
          scale = extract_scale(sql_type)
          precision = extract_precision(sql_type)

          if scale == 0
            # FIXME: Remove this class as well
            ActiveRecord::Type::DecimalWithoutScale.new(precision: precision)
          else
            ActiveRecord::Type::Decimal.new(precision: precision, scale: scale)
          end
        end

        m.alias_type %r(xml)i,      'text'
        m.alias_type %r(for bit data)i,      'binary'
        m.alias_type %r(serial)i,      'int'
        m.alias_type %r(decfloat)i,      'decimal'
        m.alias_type %r(real)i,      'decimal'
        m.alias_type %r(graphic)i,      'binary'
        m.alias_type %r(rowid)i,      'int'
      end

      def extract_table_ref_from_insert_sql(sql)
        sql.split(/\s+/)[2]
      end

      # in DB/2 for i we need to wrap the INSERT in a SELECT to get auto-generated values
      # we need this to retrieve generated id
      def sql_for_insert(sql, pk, binds)
        unless pk
          table_ref = extract_table_ref_from_insert_sql(sql)
          pk = primary_key(table_ref) if table_ref
        end

        sql = "select #{quote_column_name(pk)} from final table (#{sql})" if pk
        [sql, binds]
      end
    end
  end
end

# we took part of this code from ruby-ibmdb (Apache License)
module Arel
  module Visitors
    class Visitor
    end

    class ODBC_AS400 < Arel::Visitors::ToSql
      private
        def visit_Arel_Nodes_Limit(o, collector)
          collector << " LIMIT "
          visit(o.expr, collector)
        end

        def visit_Arel_Nodes_Offset(o, collector)
          collector << " OFFSET "
          visit o.expr, collector
        end

        def visit_Arel_Nodes_SelectStatement o, collector
          if o.with
            collector = visit o.with, collector
            collector << " "
          end

          collector = o.cores.inject(collector) { |c,x|
            visit_Arel_Nodes_SelectCore(x, c)
          }

          unless o.orders.empty?
            collector << " ORDER BY "
            len = o.orders.length - 1
            o.orders.each_with_index { |x, i|
              collector = visit(x, collector)
              collector << ", " unless len == i
            }
          end

          if (o.offset && o.limit)
            visit_Arel_Nodes_Limit(o.limit, collector)
            visit_Arel_Nodes_Offset(o.offset, collector)
          elsif (o.offset && o.limit.nil?)
            collector << " OFFSET "
            visit o.offset.expr, collector
            collector << " ROWS "
            maybe_visit o.lock, collector
          else
            visit_Arel_Nodes_SelectOptions(o, collector)
          end
        end

        # Locks are not supported in DB2
        def visit_Arel_Nodes_Lock(o, collector)
          collector
        end

        # implement case insensitive matches
#        def visit_Arel_Nodes_Matches(o, collector)
#          op = o.case_sensitive ? " LIKE " : " LIKE "
#          collector = "UPPER(", visit o.left, ")", collector
#          collector << op
#          collector = "UPPER(", infix_value o, ")", 
#          if o.escape
#            collector << " ESCAPE "
#            visit o.escape, collector
#          else
#            collector
#          end
#        end

    end
  end
end
