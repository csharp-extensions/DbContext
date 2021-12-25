using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking.Internal;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using System.Reactive.Linq;
using MySqlConnector;
using Newtonsoft.Json.Linq;

namespace CSharpExtensions.OpenSource.DbContext
{
    public static class DbContextExtensions
    {

        public static IOrderedQueryable<T> SortBy<T, TS>(this IQueryable<T> q, Expression<Func<T, TS>> keySelector, bool asc = true)
        {

            if (!asc)
            {
                return q.OrderByDescending(keySelector);
            }
            return q.OrderBy(keySelector);
        }

        public static bool SchemaCheck(this Microsoft.EntityFrameworkCore.DbContext dbContext, List<string>? excludeList = null)
        {
            excludeList ??= new List<string>();
            MethodInfo dbContextSetMethod = typeof(Microsoft.EntityFrameworkCore.DbContext).GetMethod(nameof(Microsoft.EntityFrameworkCore.DbContext.Set), BindingFlags.Public | BindingFlags.Instance);
            var dbSetsPropsTypes = dbContext.GetType().GetProperties()
                .Where(x => x.PropertyType.Name.ToLower().Contains("dbset") && !excludeList.Any(y => y.ToLower() == x.Name.ToLower()))
                .Select(x => x.PropertyType).ToList();

            dbContext.Database.SetCommandTimeout(20000);
            foreach (var prop in dbSetsPropsTypes)
            {
                var dbSetType = prop.GetGenericArguments()[0];
                var dbSetMethod = dbContextSetMethod?.MakeGenericMethod(dbSetType);

                try
                {
                    var b = dbSetMethod?.Invoke(dbContext, null) as IQueryable<object>;
                    _ = b?.FirstOrDefault();
                }
                catch (Exception ex)
                {
                    throw new Exception(dbSetType.FullName, ex);
                }

            }
            return true;
        }

        public static Task<List<T>> GetAllByPartitions<T>(this DbSet<T> dbSet, int itemsPerPartitions = 1000) where T : class, new()
            => GetAllByPartitions(dbSet.AsQueryable(), dbSet.GetTableName(), itemsPerPartitions);
        public static async Task<List<T>> GetAllByPartitions<T>(this IQueryable<T> q, string tableName = "table", int itemsPerPartitions = 1000)
        {
            var lst = new List<T>();
            await q.ExecAllByPartitions(data => { lst.AddRange(data); return Task.CompletedTask; }, tableName, itemsPerPartitions);
            return lst;
        }

        public static Task ExecAllByPartitions<T>(this DbSet<T> dbSet, Func<List<T>, Task> exec, int itemsPerPartitions = 1000, int? tableCount = null, int? startIndex = null, bool skipTableCount = true, Action<string>? logger = null) where T : class, new()
            => ExecAllByPartitions(dbSet.AsQueryable(), exec, dbSet.GetTableName(), itemsPerPartitions, tableCount, startIndex, skipTableCount, logger);
        public static async Task ExecAllByPartitions<T>(this IQueryable<T> q, Func<List<T>, Task> exec, string tableName = "table", int itemsPerPartitions = 1000, int? tableCount = null, int? startIndex = null, bool skipTableCount = true, Action<string>? logger = null)
        {
            logger = logger ?? (str => { });
            tableCount ??= !skipTableCount ? await q.CountAsync() : null;
            int i = startIndex == null ? 0 : (int)Math.Floor((1.0 * startIndex.Value) / itemsPerPartitions);
            int? times = tableCount == null ? null : (int)Math.Ceiling((1.0 * tableCount.Value) / itemsPerPartitions);
            int? leftItems = tableCount == null ? null : tableCount - (i * itemsPerPartitions);
            logger($"ExecAllByPartitions - {tableName} - {tableCount:n0} total items{(i > 0 ? $" skiped {tableCount:n0 - leftItems:n0} items" : "")} - {times:n0} gets {(i > 0 ? $" skiped {i}" : "")}, {itemsPerPartitions} per get");
            var total = 0;
            while (true)
            {
                if (times != null && i > times) { return; }
                var indexStr = times != null ? $"{i:n0}/{times:n0}" : $"iteration num {i:n0}";
                logger($"ExecAllByPartitions - {tableName} - start {indexStr}, total exec {total:n0}{(leftItems != null ? $"/{leftItems:n0}" : "")}");
                var tmp = await q.Skip(i * itemsPerPartitions).Take(itemsPerPartitions).ToListAsync();
                await exec(tmp);
                total += tmp.Count;
                logger($"ExecAllByPartitions - {tableName} - finish {indexStr}, total exec {total:n0}{(leftItems != null ? $"/{leftItems:n0}" : "")}");
                if (tmp.Count <= 0) { return; }
                i++;
            }
        }

        public static string GetTableName<T>(this DbSet<T> dbSet) where T : class
        {
            if (typeof(T).GetCustomAttributes(typeof(TableAttribute), true).FirstOrDefault() is TableAttribute dnAttribute)
            {
                return dnAttribute.Name;
            }
            return typeof(T).Name;
        }
        public static PropertyInfo[] GetPrimaryKeys<T>(this DbSet<T> dbSet) where T : class => GetPrimaryKeys(default(T));
        public static Expression<Func<T, bool>> BuildPredicte<T>(List<string> propsNames, params T[] newItems) where T : class
        {
            var propertyInfos = typeof(T).GetProperties().Where(x => propsNames.Any(y => x.Name.ToLower() == y.ToLower()));
            var existingItemParam = Expression.Parameter(typeof(T), "e");
            var itemBodies = new List<BinaryExpression>();
            foreach (var newItem in newItems)
            {
                var propsComparation = propertyInfos.Select(propKey =>
                    Expression.Equal(
                        Expression.Property(existingItemParam, propKey.Name),
                        Expression.Constant(propKey.GetValue(newItem), propKey.PropertyType)
                    )
                );
                itemBodies.Add(propsComparation.Aggregate(Expression.AndAlso));
            }
            var body = newItems.Count() == 1 ? itemBodies.First() : itemBodies.Aggregate(Expression.Or);
            return Expression.Lambda<Func<T, bool>>(body, existingItemParam);
        }

        public static PropertyInfo[] GetPrimaryKeys<T>(this T? entity) where T : class
        {
            var type = entity?.GetType() ?? typeof(T);
            var pk = type.GetProperties().Where(prop => prop.GetCustomAttributes(typeof(KeyAttribute), true).Any()).ToList();
            if (pk.Count > 0)
            {
                return pk.ToArray();
            }
            throw new Exception($"The table of the object {type} do not have PK.");
            //return typeof(T).GetProperties();
        }

        public static async Task<(List<T> inserted, List<T> updated, List<T> unchanged, List<T> deleted, long ms)> BulkInsertUpdateOrDelete<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, List<T> data, List<string>? propsToCompare = null, List<string>? excludePropsList = null, Action<List<T>>? delAction = null, Action<List<T>>? postInstert = null, Action<List<T>>? postUpdated = null, Action<List<T>>? postUnchanged = null, bool disableUpdate = false, bool disableLog = false, bool disableDelete = false, Action<string>? logger = null) where T : class, new()
        {
            logger = logger ?? (str => { });
            if (data == null)
            {
                return (new List<T>(), new List<T>(), new List<T>(), new List<T>(), 0);
            }
            var dbSet = dbContext.Set<T>();
            var AutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
            dbContext.ChangeTracker.AutoDetectChangesEnabled = true;
            var tableName = dbSet.GetTableName();
            logger($"{tableName} - start bulking");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var existingData = await dbSet.AsNoTracking().ToListAsync();
            logger($"{tableName} - existingData - {existingData.Count} items");

            List<(T exist, T newItem)> updatedWithExists = new List<(T exist, T newItem)>();
            List<T> inserted = new List<T>();
            List<T> unchanged = new List<T>();

            propsToCompare = propsToCompare.GetEmptyIfNull().Any() ? propsToCompare : dbSet.GetPrimaryKeys().Select(x => x.Name).ToList();

            foreach (var newItem in data)
            {
                bool found = false;
                foreach (var existItem in existingData)
                {
                    var expression = BuildPredicte(propsToCompare!, newItem);
                    if (expression.Compile()(existItem))
                    {
                        if (existItem.Compare(newItem, excludePropsList))
                        {
                            unchanged.Add(existItem);
                        }
                        else
                        {
                            updatedWithExists.Add((existItem, newItem));
                        }
                        if (!disableUpdate)
                        {
                            dbSet.Attach(existItem);
                        }
                        found = true;
                        break;
                    }
                }
                if (!found)
                {
                    inserted.Add(newItem);
                }
            }

            var updated = updatedWithExists.Select(x => x.exist).ToList();
            if (!disableUpdate)
            {
                updatedWithExists.ForEach(x => x.newItem.CopyTo(x.exist, excludePropsList));
            }

            var deleted = existingData.Except(updatedWithExists.Select(x => x.exist)).Except(unchanged).ToList();
            if (!disableDelete)
            {
                deleted.ForEach(x => dbSet.Attach(x));
            }

            logger($"{tableName} - detect inserted={inserted.Count}, updated={updated.Count}, unchanged={unchanged.Count}, deleted={deleted.Count}");

            postInstert?.Invoke(inserted);
            await dbSet.AddRangeAsync(inserted);
            delAction ??= (List<T> lst) => dbSet.RemoveRange(lst);
            delAction(deleted);
            postUpdated?.Invoke(updated);
            postUnchanged?.Invoke(unchanged);

            logger($"{tableName} - post actions");

            logger($"{tableName} - start saving");
            await dbContext.SaveChangesAsync();
            logger($"{tableName} - saved");

            stopwatch.Stop();

            var result = (inserted, updated, unchanged, deleted, ms: stopwatch.ElapsedMilliseconds);

            if (!disableLog)
            {
                logger($"{typeof(T).Name} - inserted={inserted.Count}, updated={updated.Count}, unchanged={unchanged.Count}, deleted={deleted.Count} takes timespan = {TimeSpan.FromMilliseconds(result.ms)}");
            }
            dbContext.ChangeTracker.AutoDetectChangesEnabled = AutoDetectChangesEnabled;
            dbContext.Disattached();
            return result;
        }

        public static void RevertChanges(this Microsoft.EntityFrameworkCore.DbContext context)
        {
            var changedEntries = context.ChangeTracker.Entries()
                                        .Where(x => x.State != EntityState.Unchanged).ToList();

            foreach (var entry in changedEntries)
            {
                switch (entry.State)
                {
                    case EntityState.Modified:
                        entry.CurrentValues.SetValues(entry.OriginalValues);
                        entry.State = EntityState.Unchanged;
                        break;

                    case EntityState.Added:
                        entry.State = EntityState.Detached;
                        break;

                    case EntityState.Deleted:
                        entry.State = EntityState.Unchanged;
                        break;
                }
            }
        }

        public static void Disattached(this Microsoft.EntityFrameworkCore.DbContext context)
        {
            context.ChangeTracker.Entries().ToList().ForEach(x => x.State = EntityState.Detached);
        }

        public static async Task<(List<T> inserted, List<T> updated, List<T> unchanged, long ms)> BulkInsertOrUpdate<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, List<T> data, List<string>? propsToCompare = null, List<string>? excludePropsList = null, Action<List<T>>? postInstert = null, Action<List<T>>? postUpdated = null, Action<List<T>>? postUnchanged = null, bool disableUpdate = false, bool disableLog = false, Action<string>? logger = null) where T : class, new()
        {
            logger = logger ?? (str => { });
            if (!data.GetEmptyIfNull().Any())
            {
                return (new(), new(), new(), 0);
            }
            var dbSet = dbContext.Set<T>();
            var AutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
            dbContext.ChangeTracker.AutoDetectChangesEnabled = true;
            var QueryTrackingBehavior = dbContext.ChangeTracker.QueryTrackingBehavior;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            var tableName = dbSet.GetTableName();
            logger($"{tableName} - start bulking");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var inserted = new List<T>();
            var unchanged = new List<T>();
            var updated = new List<T>();

            propsToCompare = propsToCompare.GetEmptyIfNull().Any() ? propsToCompare : dbSet.GetPrimaryKeys().Select(x => x.Name).ToList();

            var index = 0;
            var dataWithPredictes = data.Select(x => (item: x, predicte: BuildPredicte(propsToCompare!, x)));
            foreach (var (item, predicte) in dataWithPredictes)
            {
                var match = await dbSet.SingleOrDefaultAsync(predicte);
                if (match == null)
                {
                    await dbContext.AddAsync(item);
                    inserted.Add(item);
                }
                else if (!disableUpdate)
                {
                    var existItem = match!;
                    if (existItem.Compare(item, excludePropsList))
                    {
                        unchanged.Add(existItem);
                    }
                    else
                    {
                        dbSet.Attach(item);
                        item.CopyTo(existItem, excludePropsList);
                        updated.Add(existItem);
                    }
                }
                index++;
                if (index % 1000 == 0)
                {
                    logger($"{tableName} - start saving {index}/{data.Count}");
                    await dbContext.SaveChangesAsync();
                    logger($"{tableName} - saved {index}/{data.Count}");
                }
                index++;
            }

            await dbContext.AddRangeAsync(inserted);
            updated.ForEach(x => dbSet.Attach(x));
            dbContext.ChangeTracker.DetectChanges();

            logger($"{tableName} - detect inserted={inserted.Count}, updated={updated.Count}, unchanged={unchanged.Count}");

            logger($"{tableName} - start saving {index}/{data.Count}");
            await dbContext.SaveChangesAsync();
            logger($"{tableName} - saved {index}/{data.Count}");

            postInstert?.Invoke(inserted);
            postUpdated?.Invoke(updated);
            postUnchanged?.Invoke(unchanged);

            logger($"{tableName} - post actions");

            logger($"{tableName} - start saving");
            await dbContext.SaveChangesAsync();
            logger($"{tableName} - saved");

            stopwatch.Stop();

            var result = (inserted, updated, unchanged, ms: stopwatch.ElapsedMilliseconds);

            if (!disableLog)
            {
                logger($"{typeof(T).Name} - inserted={inserted.Count}, updated={updated.Count}, unchanged={unchanged.Count} takes timespan = {TimeSpan.FromMilliseconds(result.ms)}");
            }

            dbContext.ChangeTracker.AutoDetectChangesEnabled = AutoDetectChangesEnabled;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior;
            dbContext.Disattached();
            return result;
        }
        public static bool IsDuplicateError(this Exception? ex) => ex?.ToString()?.ToLower()?.Contains("duplicate entry") ?? false;

        public static async Task<(List<T> inserted, long ms)> BulkInsertIfMissing2<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, List<T> data, List<string>? propsToCompare = null, Action<List<T>>? postInstert = null, bool disableLog = false, Action<string>? logger = null) where T : class, new()
        {
            logger = logger ?? (str => { });
            if (!data.GetEmptyIfNull().Any())
            {
                return (new(), 0);
            }
            var dbSet = dbContext.Set<T>();
            var AutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
            dbContext.ChangeTracker.AutoDetectChangesEnabled = true;
            var QueryTrackingBehavior = dbContext.ChangeTracker.QueryTrackingBehavior;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior.NoTracking;
            var tableName = dbSet.GetTableName();
            logger($"{tableName} - start bulking {data.Count} items");

            Stopwatch stopwatch = new Stopwatch();
            stopwatch.Start();

            var inserted = new List<T>();

            propsToCompare = propsToCompare.GetEmptyIfNull().Any() ? propsToCompare : dbSet.GetPrimaryKeys().Select(x => x.Name).ToList();

            var index = 0;
            var dataWithPredictes = data.Select(x => (item: x, predicte: BuildPredicte(propsToCompare!, x)));
            foreach (var (item, predicte) in dataWithPredictes)
            {
                try
                {
                    await dbContext.AddAsync(item);
                    await dbContext.SaveChangesAsync();
                    inserted.Add(item);
                }
                catch (Exception ex)
                {
                    dbContext.Disattached();
                    if (!ex.IsDuplicateError())
                    {
                        throw;
                    }
                }
                index++;
                if (index % 1000 == 0)
                {
                    logger($"{tableName} - {index}/{data.Count}");
                }
            }

            postInstert?.Invoke(inserted);
            await dbContext.SaveChangesAsync();
            stopwatch.Stop();
            var result = (inserted, ms: stopwatch.ElapsedMilliseconds);

            if (!disableLog)
            {
                logger($"{typeof(T).Name} - inserted={inserted.Count} takes timespan = {TimeSpan.FromMilliseconds(result.ms)}");
            }

            dbContext.ChangeTracker.AutoDetectChangesEnabled = AutoDetectChangesEnabled;
            dbContext.ChangeTracker.QueryTrackingBehavior = QueryTrackingBehavior;
            dbContext.Disattached();
            return result;
        }

        public static async Task<(List<T> inserted, long ms)> BulkInsertIfMissing<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, List<T> data, List<string>? propsToCompare = null, Action<List<T>>? postInstert = null, bool disableLog = false, Action<string>? logger = null) where T : class, new()
        {
            logger = logger ?? (str => { });
            var result = await BulkInsertOrUpdate(dbContext, data, propsToCompare, null, postInstert, null, null, true, true);
            if (!disableLog)
            {
                logger($"{typeof(T).Name} - inserted={result.inserted.Count} takes timespan = {TimeSpan.FromMilliseconds(result.ms)}");
            }
            return (result.inserted, result.ms);
        }

        public static string PreventSqlInj(this string str) => (str ?? "").Replace("'", "''");

        public static async Task<(bool isNew, T item)> AddOrUpdate<T>(
            this Microsoft.EntityFrameworkCore.DbContext dbContext,
            T newData,
            Expression<Func<T, bool>>? predicate = null,
            bool autoSave = false,
            List<string>? excludePropsList = null,
            bool disableUpdate = false,
            bool ignoreDuplicateError = false
            ) where T : class, new()
        {
            excludePropsList ??= new List<string>();

            var dbSet = dbContext.Set<T>();
            var AutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
            dbContext.ChangeTracker.AutoDetectChangesEnabled = true;
            bool isNew = false;

            predicate ??= BuildPredicte(dbSet.GetPrimaryKeys().Select(x => x.Name).ToList(), newData);
            var search = await dbSet.FirstOrDefaultAsync(predicate);

            if (search == null)
            {
                dbSet.Add(newData);
                isNew = true;
                search = newData;
            }
            else if (!disableUpdate)
            {
                newData.CopyTo(search, excludePropsList);
            }
            if (autoSave && (isNew || !disableUpdate))
            {
                try
                {
                    await dbContext.SaveChangesAsync();
                }
                catch (Exception ex)
                {
                    dbContext.Disattached();
                    if (!ignoreDuplicateError || !ex.IsDuplicateError())
                    {
                        throw;
                    }
                }
            }
            if (autoSave)
            {
                dbContext.Disattached();
            }
            dbContext.ChangeTracker.AutoDetectChangesEnabled = AutoDetectChangesEnabled;
            return (isNew, search);
        }

        public static async Task UpdateQuery<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, Expression<Func<T, bool>> predicate, Action<T> modify, bool autoSave = true, bool ignoreDuplicateError = false) where T : class, new()
        {
            var dbSet = dbContext.Set<T>();
            var AutoDetectChangesEnabled = dbContext.ChangeTracker.AutoDetectChangesEnabled;
            dbContext.ChangeTracker.AutoDetectChangesEnabled = true;
            var items = await dbSet.Where(predicate).ToListAsync();
            foreach (var item in items)
            {
                modify(item);
                if (autoSave)
                {
                    try
                    {
                        await dbContext.SaveChangesAsync();
                    }
                    catch (Exception ex)
                    {
                        dbSet.Attach(item).State = EntityState.Detached;
                        if (!ignoreDuplicateError || !ex.IsDuplicateError())
                        {
                            throw;
                        }
                    }
                }
            }
            if (autoSave)
            {
                dbContext.Disattached();
            }
            dbContext.ChangeTracker.AutoDetectChangesEnabled = AutoDetectChangesEnabled;
        }

        public static async Task<int> Delete<T>(this Microsoft.EntityFrameworkCore.DbContext dbContext, Expression<Func<T, bool>> predicate) where T : class
        {
            var dbSet = dbContext.Set<T>();
            var lst = dbSet.Where(predicate).ToList();
            dbSet.RemoveRange(lst);
            await dbContext.SaveChangesAsync();
            return lst.Count;
        }

        public static async Task<IEnumerable<T?>> ExecuteReader<T>(this Microsoft.EntityFrameworkCore.DbContext context, string query) where T : class, new()
        {
            var entities = new List<T?>();

            var connection = context.Database.GetDbConnection();
            var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            await using DbDataReader result = command.ExecuteReader();
            while (result.Read())
            {
                entities.Add(ConvertReaderToRelevantModel<T>(result));
            }

            return entities;
        }

        public static async Task<string> SqlToJson(this Microsoft.EntityFrameworkCore.DbContext context, string query)
        {
            var connection = context.Database.GetDbConnection();
            var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
            await using DbDataReader result = await command.ExecuteReaderAsync();
            var dataTable = new DataTable();
            dataTable.Load(result);
            return JsonConvert.SerializeObject(dataTable);
        }

        public static Task<int> ExecuteNonQuery(this Microsoft.EntityFrameworkCore.DbContext context, string query, int? commandTimeout = null, bool createNewConnection = false)
        {
            var connection = context.Database.GetDbConnection();
            var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            command.CommandTimeout = commandTimeout ?? int.MaxValue;
            return context.ExecuteNonQuery(command, createNewConnection);
        }

        public static async Task<int> ExecuteNonQuery(this Microsoft.EntityFrameworkCore.DbContext context, DbCommand command, bool createNewConnection = false)
        {
            var connection = context.Database.GetDbConnection();
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
            int res = await command.ExecuteNonQueryAsync();
            if (createNewConnection && connection.State == ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
            return res;
        }

        public static async Task<T?> ExecuteScalar<T>(this Microsoft.EntityFrameworkCore.DbContext context, string query, bool createNewConnection = false)
        {
            var connection = createNewConnection
                                            ? new MySqlConnection(context.Database.GetDbConnection().ConnectionString)
                                            : context.Database.GetDbConnection();
            var command = connection.CreateCommand();
            command.CommandText = query;
            command.CommandType = CommandType.Text;
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
            var res = await command.ExecuteScalarAsync();
            if (createNewConnection && connection.State == ConnectionState.Open)
            {
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
            if (res == DBNull.Value)
            {
                return default;
            }
            return (T)res;
        }

        public static DbContextOptions<T> GetDbOptions<T>(this string connectionString) where T : Microsoft.EntityFrameworkCore.DbContext
        {
            var builder = new DbContextOptionsBuilder<T>().UseMySql(connectionString, ServerVersion.AutoDetect(connectionString), optionsBuilder =>
            {
                //optionsBuilder.EnableRetryOnFailure();
                optionsBuilder.CommandTimeout(int.MaxValue);
            });
            builder.EnableSensitiveDataLogging();
            builder.EnableDetailedErrors();

            return builder.Options;
        }

        public static T NewDbContexts<T>(this string connectionString) where T : Microsoft.EntityFrameworkCore.DbContext
        {
            var options = connectionString.GetDbOptions<T>();
            T newDbContext = (T)Activator.CreateInstance(typeof(T), options);
            return newDbContext;
        }

        public static T NewDbContexts<T>(this T dbContext) where T : Microsoft.EntityFrameworkCore.DbContext
        {
            var connection = dbContext.Database.GetDbConnection();
            T newDbContext = (T)Activator.CreateInstance(typeof(T), GetDbOptions<T>(connection.ConnectionString));
            return newDbContext;
        }

        public static T? ConvertReaderToRelevantModel<T>(DbDataReader reader) where T : class, new()
        {
            T res = new T();
            if (res is JObject or JArray or JToken)
            {
                return JsonConvert.DeserializeObject<T>(JsonConvert.SerializeObject(reader));
            }
            foreach (var item in res.GetType().GetProperties())
            {
                var prop = res.GetType().GetProperty(item.Name)!;
                string objectPropType = prop.PropertyType.Name;
                string readerPropType = reader[item.Name].GetType().Name;
                object? readerValue = reader[item.Name] == DBNull.Value ? null : reader[item.Name];
                readerValue ??= "";
                try
                {
                    if (objectPropType == typeof(DateTime).Name && readerPropType == typeof(string).Name)
                    {
                        prop.SetValue(res, readerValue.ToString()!.ToDate(), null);
                    }
                    else
                    {
                        prop.SetValue(res, readerValue, null);
                    }
                }
                catch
                {
                    throw new Exception($"cant convert reader Property to Object Property {item.Name}:{readerPropType} to {prop.Name}:{objectPropType} ");
                }
            }
            return res;
        }

        public static DateTime? ToDate(this object input)
        {
            try
            {
                return DateTime.Parse(input.ToString()!);
            }
            catch
            {
                return null;
            }
        }
    }
}
