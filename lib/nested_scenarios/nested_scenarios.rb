class NestedScenarios
  cattr_accessor :record_name_fields, :skip_tables
  @@record_name_fields = %w( name username title )
  @@skip_tables        = %w( schema_migrations )

  def self.delete_tables(table_names = self.tables)
    connection = ActiveRecord::Base.connection
    ActiveRecord::Base.silence do
      connection.disable_referential_integrity do
        tables.each do |table_name|
          connection.delete "DELETE FROM #{table_name}", 'Fixture Delete'
        end
      end
    end
  end

  def self.existing_ids
    connection = ActiveRecord::Base.connection
    tables.inject({}) do |h,table_name|
      h[table_name] =
        connection.select_values "SELECT id FROM #{table_name}", 'Fixture Select'
      h
    end
  end

  def self.tables(table_names = ActiveRecord::Base.connection.tables - @@skip_tables)
    t_names = (table_names - @@skip_tables)
    return t_names unless block_given? 
    t_names.each { |table_name| yield table_name }
  end
end
