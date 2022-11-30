module ODBCAdapter
  module Tasks

    def self.register_tasks(pattern, task)
      ActiveRecord::Tasks::DatabaseTasks.register_task(pattern, task)
    end

    require 'odbc_adapter/tasks/as400_odbc_database_tasks'
    register_tasks(/odbc/, ODBCAdapter::Tasks::As400OdbcDatabaseTasks)
  end
end
