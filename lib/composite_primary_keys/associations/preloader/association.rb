module ActiveRecord
  module Associations
    class Preloader
      class Association
        def records_for(ids)
          # CPK
          # scope.where(association_key.in(ids))
          predicate = cpk_in_predicate(table, reflection.foreign_key, ids)
          scope.where(predicate)
        end

        def associated_records_by_owner
          # CPK
          owners_map = owners_by_key
          #owner_keys = owners_map.keys.compact
          owner_keys = owners.map do |owner|
            Array(owner_key_name).map do |owner_key|
              owner[owner_key]
            end
          end.compact.uniq

          if klass.nil? || owner_keys.empty?
            records = []
          else
            # Some databases impose a limit on the number of ids in a list (in Oracle it's 1000)
            # Make several smaller queries if necessary or make one query if the adapter supports it
            sliced  = owner_keys.each_slice(model.connection.in_clause_length || owner_keys.size)
            records = sliced.map { |slice| records_for(slice) }.flatten
          end

          # Each record may have multiple owners, and vice-versa
          records_by_owner = Hash[owners.map { |owner| [owner, []] }]
          records.each do |record|
            # CPK
            # owner_key = record[association_key_name].to_s
            owner_key = Array(association_key_name).map do |key_name|
              record[key_name].to_s.downcase.rstrip
            end.join(CompositePrimaryKeys::ID_SEP)

            # Code requires Raven (Sentry to be setup) and handles the
            # transliterated keys.
            if !owners_map.has_key?(owner_key)
              # Send error to Sentry for bad key
              e = StandardError.new("#{owner_key} was not in map")
              Raven.capture_exception(e)

              # Add transliterated key
              transliterate_keys(owners_map)
              # Mutate the owner_key to the transliterated version
              owner_key = I18n.transliterate(owner_key)
            end
            owners_map[owner_key].each do |owner|
              records_by_owner[owner] << record
            end
          end
          records_by_owner
        end

        def owners_by_key
          @owners_by_key ||= owners.group_by do |owner|
            # CPK
            # key = owner[owner_key_name]
            key = Array(owner_key_name).map do |key_name|
              owner[key_name].to_s.downcase.rstrip
            end
            # CPK
            # key && key.to_s
            key && key.join(CompositePrimaryKeys::ID_SEP)
          end
        end

        # Takes the owners_map and adds all the transliterated versions
        # of the keys to reference the same values. It will not allow transliterated
        # keys to collid with an original key or another transliterated key. It
        # mutates the owners map.
        def transliterate_keys(owners_map)
          transliterated_keys = {}
          duplicated_keys = Set.new
          owners_map.keys.each do |k|
            transliterated_key = I18n.transliterate(k)
            next if transliterated_key == k # No originals
            if transliterated_keys.include?(transliterated_key)
              duplicated_keys.add(transliterated_key)
            elsif !owners_map.include?(transliterated_key)
              transliterated_keys[transliterated_key] = owners_map[k]
            end
          end
          duplicated_keys.each do |k|
            transliterated_keys.delete(k)
          end
          owners_map.merge!(transliterated_keys)
          transliterated_keys
        end

      end
    end
  end
end
