package org.finos.legend.pylegend;
import com.fasterxml.jackson.core.type.TypeReference;
import com.hazelcast.console.LineReader;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.factory.Maps;
import org.finos.legend.engine.plan.execution.PlanExecutor;
import org.finos.legend.engine.plan.execution.stores.relational.config.RelationalExecutionConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.config.TemporaryTestDbConfiguration;
import org.finos.legend.engine.plan.execution.stores.relational.connection.authentication.strategy.OAuthProfile;
import org.finos.legend.engine.plan.execution.stores.relational.connection.manager.ConnectionManagerSelector;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.Relational;
import org.finos.legend.engine.plan.execution.stores.relational.plugin.RelationalStoreExecutor;
import org.finos.legend.engine.protocol.pure.v1.model.packageableElement.store.relational.connection.RelationalDatabaseConnection;
import java.util.Collections;
import org.finos.legend.engine.repl.client.jline3.JLine3Completer;
import org.finos.legend.engine.repl.client.jline3.JLine3Parser;
import org.finos.legend.engine.repl.core.legend.LegendInterface;
import org.finos.legend.engine.repl.core.legend.LocalLegendInterface;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.*;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.util.*;
import java.util.stream.Collectors;
import org.finos.legend.engine.repl.client.Client;
import java.sql.*;

import org.finos.legend.engine.repl.dataCube.DataCubeReplExtension;
import org.finos.legend.engine.repl.relational.RelationalReplExtension;
import org.finos.legend.engine.repl.relational.autocomplete.RelationalCompleterExtension;
import org.finos.legend.engine.repl.relational.shared.ConnectionHelper;
import org.jline.reader.LineReaderBuilder;

import java.sql.Statement;
import static org.finos.legend.engine.protocol.store.elasticsearch.v7.specification.ElasticsearchObjectMapperProvider.OBJECT_MAPPER;


@Path("/data")
public class LoadController
{
    // Simulated in-memory database
    private static final Map<String, Object> dataStore = new HashMap<>();
    @POST
    @Path("/createtable")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadTable(String payload)
    {
        if (!payload.startsWith("create "))
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Payload must start with 'create '")).build();
        }
        String rest = payload.substring(6).trim();
        int firstParen = rest.indexOf('(');
        int lastParen = rest.lastIndexOf(')');
        if (firstParen == -1 || lastParen == -1 || lastParen <= firstParen)
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Missing or malformed schema declaration")).build();
        }
        String pathPart = rest.substring(0, firstParen).trim();
        String schemaPart = rest.substring(firstParen + 1, lastParen).trim();
        String[] parts = pathPart.split("::");
        if (parts.length != 4)
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Path must be warehouse::db::schema::table")).build();
        }
        String warehouse = parts[0], db = parts[1], schema = parts[2], table = parts[3];
        // Parse schema
        // Parse schema
        List<ServerState.Column> columns = new ArrayList<>();
        String[] columnDefs = schemaPart.split(",");
        for (String colDef : columnDefs)
        {
            colDef = colDef.trim();
            String[] tokens = colDef.split("\\s+");
            if (tokens.length < 2)
            {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid column definition: " + colDef)).build();
            }
            String name = tokens[0];
            String type = null;
            boolean isPrimaryKey = false;
            // Find all brackets: [type], [primarykey]
            for (String token : tokens)
            {
                if (token.matches("\\[.*\\]"))
                {
                    String content = token.substring(1, token.length() - 1).toLowerCase();
                    if (content.equals("primarykey") || content.equals("primary_key"))
                    {
                        isPrimaryKey = true;
                    }
                    else
                    {
                        type = content;
                    }
                }
            }

            if (type == null)
            {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Missing or invalid type for column: " + name)).build();
            }

            columns.add(new ServerState.Column(name, type, isPrimaryKey));
        }

        // Store it
        ServerState.data
                .computeIfAbsent(warehouse, k -> new HashMap<>())
                .computeIfAbsent(db, k -> new HashMap<>())
                .computeIfAbsent(schema, k -> new HashMap<>())
                .put(table, new ServerState.TableMetadata(columns));

        return Response.ok(Map.of(
                "message", "Table initialized with schema",
                "warehouse", warehouse,
                "database", db,
                "schema", schema,
                "table", table,
                "columns", columns.stream().map(c -> Map.of(
                        "name", c.name,
                        "type", c.type,
                        "primaryKey", c.isPrimaryKey
                )).collect(Collectors.toList())
        )).build();
    }
    @POST
    @Path("/deleterow")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response deleteRowByIndex(String input)
    {
        try
        {
            // Expect input: "warehouse::db::schema::table::index"
            String[] parts = input.trim().split("::");
            if (parts.length != 5)
            {
                throw new IllegalArgumentException("Invalid format. Expected: warehouse::db::schema::table::index");
            }

            String warehouse = parts[0];
            String db = parts[1];
            String schema = parts[2];
            String table = parts[3];
            int index = Integer.parseInt(parts[4]);

            Map<String, Map<String, Map<String, ServerState.TableMetadata>>> dbs = ServerState.data.get(warehouse);
            if (dbs == null || !dbs.containsKey(db) || !dbs.get(db).containsKey(schema) || !dbs.get(db).get(schema).containsKey(table))
            {
                throw new RuntimeException("Table not found: " + String.join("::", warehouse, db, schema, table));
            }

            ServerState.TableMetadata metadata = dbs.get(db).get(schema).get(table);
            List<Map<String, Object>> rows = metadata.rows;

            if (index < 0 || index >= rows.size())
            {
                throw new IndexOutOfBoundsException("Row index out of range: " + index);
            }

            Map<String, Object> removedRow = rows.remove(index);

            return Response.ok(Map.of(
                    "message", "Row deleted successfully",
                    "deletedRow", removedRow
            )).build();
        }
        catch (Exception e)
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage()))
                    .build();
        }
    }

    @POST
    @Path("/insertrow")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response insert(Map<String, Object> payload)
    {
        try
        {
            // Step 1: Extract and validate path
            String fullPath = (String) payload.get("path");
            if (fullPath == null || !fullPath.contains("::"))
            {
                throw new RuntimeException("Missing or malformed 'path' field. Expected format: warehouse::database::schema::table");
            }

            String[] parts = fullPath.split("::");
            if (parts.length != 4)
            {
                throw new RuntimeException("Invalid path format. Expected: warehouse::database::schema::table");
            }

            String warehouse = parts[0], db = parts[1], schema = parts[2], table = parts[3];

            // Step 2: Get row
            Object rowObj = payload.get("row");
            if (!(rowObj instanceof Map))
            {
                throw new RuntimeException("Missing or invalid 'row' field");
            }

            @SuppressWarnings("unchecked")
            Map<String, Object> row = (Map<String, Object>) rowObj;

            // Step 3: Locate the table
            Map<String, Map<String, Map<String, ServerState.TableMetadata>>> dbs = ServerState.data.get(warehouse);
            if (dbs == null || !dbs.containsKey(db) || !dbs.get(db).containsKey(schema) || !dbs.get(db).get(schema).containsKey(table))
            {
                throw new RuntimeException("Table not found: " + fullPath);
            }

            ServerState.TableMetadata tableMeta = dbs.get(db).get(schema).get(table);
            List<ServerState.Column> columns = tableMeta.schema;

            // Step 4: Validate all required fields and types
            for (ServerState.Column col : columns)
            {
                if (!row.containsKey(col.name))
                {
                    throw new RuntimeException("Missing required column: " + col.name);
                }

                Object value = row.get(col.name);
                if (!isTypeMatch(value, col.type))
                {
                    throw new RuntimeException("Type mismatch for column: " + col.name + ". Expected: " + col.type + ", Got: " + value.getClass().getSimpleName());
                }
            }

            // Step 5: Check for duplicate primary key
            for (ServerState.Column col : columns)
            {
                if (col.isPrimaryKey)
                {
                    Object pkValue = row.get(col.name);
                    for (Map<String, Object> existingRow : tableMeta.rows)
                    {
                        if (existingRow.get(col.name).equals(pkValue))
                        {
                            throw new RuntimeException("Duplicate primary key value for column: " + col.name + " = " + pkValue);
                        }
                    }
                }
            }

            // Step 6: Insert the row
            tableMeta.rows.add(row);
            return Response.ok(Map.of("message", "Row added", "row", row)).build();
        }
        catch (Exception e)
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }

    @POST
    @Path("/fetchtable")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTableDataPost(String fullPath)
    {
        try
        {
            // Parse fullPath -> warehouse::db::schema::table
            String[] parts = fullPath.trim().split("::");
            if (parts.length != 4)
            {
                throw new IllegalArgumentException("Invalid path format. Expected format: warehouse::db::schema::table");
            }

            String warehouse = parts[0];
            String db = parts[1];
            String schema = parts[2];
            String table = parts[3];

            // Check if table exists
            Map<String, Map<String, Map<String, ServerState.TableMetadata>>> dbs = ServerState.data.get(warehouse);
            if (dbs == null || !dbs.containsKey(db) || !dbs.get(db).containsKey(schema) || !dbs.get(db).get(schema).containsKey(table))
            {
                throw new RuntimeException("Table not found: " + fullPath);
            }
            ServerState.TableMetadata tableMeta = dbs.get(db).get(schema).get(table);
            List<Map<String, Object>> tableData = tableMeta.rows;
            return Response.ok(tableData).build();
        }
        catch (Exception e)
        {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }

    @GET
    @Path("/showtables")
    @Produces(MediaType.APPLICATION_JSON)
        public Response showAllTables()
        {
            List<String> tablePaths = new ArrayList<>();

            for (Map.Entry<String, Map<String, Map<String, Map<String, ServerState.TableMetadata>>>> warehouseEntry : ServerState.data.entrySet())
            {
                String warehouse = warehouseEntry.getKey();
                for (Map.Entry<String, Map<String, Map<String, ServerState.TableMetadata>>> dbEntry : warehouseEntry.getValue().entrySet())
                {
                    String db = dbEntry.getKey();
                    for (Map.Entry<String, Map<String, ServerState.TableMetadata>> schemaEntry : dbEntry.getValue().entrySet())
                    {
                        String schema = schemaEntry.getKey();
                        for (String table : schemaEntry.getValue().keySet())
                        {
                            String fullPath = String.join("::", warehouse, db, schema, table);
                            tablePaths.add(fullPath);
                        }
                    }
                }
            }
            Map<String, Object> response = new HashMap<>();
            response.put("tables", tablePaths);
            response.put("count", tablePaths.size());
            return Response.ok(response).build();
        }

    @POST
    @Path("/duckdb/load")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadCsvFromPath(String fullPath)
    {
        try
        {
            fullPath = fullPath.trim();
            File csvFile = new File(fullPath);
            if (!csvFile.exists() || !csvFile.getName().endsWith(".csv"))
            {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity("Invalid CSV file path: " + fullPath).build();
            }

            // Extract table name and create .duckdb path
            java.nio.file.Path csvPath = csvFile.toPath();
            String baseName = csvFile.getName().replaceAll("\\.csv$", ""); // 'employees'
            String tableName = baseName;
            java.nio.file.Path parentDir = csvPath.getParent();
            java.nio.file.Path duckDbPath = parentDir.resolve(baseName + ".duckdb"); // 'employees.duckdb'

            // Load into DuckDB
            Class.forName("org.duckdb.DuckDBDriver");
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + duckDbPath.toAbsolutePath());
                 Statement stmt = conn.createStatement())
            {
                String query = String.format(
                        "CREATE OR REPLACE TABLE \"%s\" AS SELECT * FROM read_csv_auto('%s');",
                        tableName,
                        csvPath.toAbsolutePath().toString().replace("\\", "\\\\")
                );
                stmt.execute(query);
            }

            return Response.ok("CSV loaded as table '" + tableName + "' in file: " + duckDbPath).build();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity("Error: " + e.getMessage()).build();
        }
    }
    @POST
    @Path("/duckdb/query")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response queryDuckDBTable(Map<String, String> request)
    {
        try
        {
            String dbPath = request.get("dbPath").trim();   // full path to .duckdb file
            String sql = request.get("query").trim();        // SQL query

            if (!dbPath.endsWith(".duckdb") || sql.isEmpty())
            {
                return Response.status(Response.Status.BAD_REQUEST)
                        .entity(Map.of("error", "Invalid input. Expecting .duckdb file and non-empty query.")).build();
            }

            File dbFile = new File(dbPath);
            if (!dbFile.exists())
            {
                return Response.status(Response.Status.NOT_FOUND)
                        .entity(Map.of("error", "DuckDB file not found at path: " + dbPath)).build();
            }

            List<Map<String, Object>> result = new ArrayList<>();
            try (Connection conn = DriverManager.getConnection("jdbc:duckdb:" + dbFile.getAbsolutePath());
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(sql))
            {
                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                while (rs.next())
                {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++)
                    {
                        row.put(meta.getColumnLabel(i), rs.getObject(i));
                    }
                    result.add(row);
                }
            }

            return Response.ok(result).build();
        }
        catch (Exception e)
        {
            e.printStackTrace();
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                    .entity(Map.of("error", e.getMessage())).build();
        }
    }

//    @POST
//    @Path("/execute")
//    public Response load(@Context HttpServletRequest request, @Context HttpServletResponse response)
//    {
//        try
//        {
//            Map<String,Object> payload = OBJECT_MAPPER.readValue(request.getInputStream(), new TypeReference<Map<String, Object>>(){});
//            String line = (String) payload.get("line");
//            if(line.startsWith("load "))
//            {
//                String[] tokens = line.split(" ");
//                if(tokens.length < 3 || tokens.length > 4)
//                {
////                    throw new RuntimeException(String.format("Error, load should be used as '%s",CLIENT.commands.selectInstancesOf(Load.class).get(0).documentation));
//                     throw new RuntimeException(String.format("Error in using the load command"));
//                }
//                RelationalDatabaseConnection databaseConnection = (RelationalDatabaseConnection) org.finos.legend.engine.repl.relational.shared.ConnectionHelper.getDatabaseConnection(CLIENT.getModelState().parse(),tokens[2]);
//                try(Connection connection = org.finos.legend.engine.repl.relational.shared.ConnectionHelper.getConnection(databaseConnection,PLAN_EXECUTOR))
//                {
//                    String tableName = tokens.length == 4 ? tokens[3] : ("test"+(ConnectionHelper.getTables(databaseConnection,PLAN_EXECUTOR).count()+1));
//                    int rowCount = 0;
//                    try(Statement statement = connection.createStatement())
//                    {
//                        statement.executeUpdate(org.finos.legend.engine.plan.execution.stores.relational.connection.driver.DatabaseManager.fromString(databaseConnection.type.name()).relationalDatabaseSupport().load(tableName, tokens[1]));
//                        try (ResultSet rs = statement.executeQuery("select count(*) as cnt from " + tableName)) {
//                            rs.next();
//                            rowCount = rs.getInt(1);
//                        }
//                    }
//                    return Response.ok("Loaded into table: '" + tableName + "'. RowCount: "+rowCount).build();
//                }
//            }
//        }
//        catch (Exception e)
//        {
//            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(Collections.singletonMap("error", e.getMessage())).type(MediaType.APPLICATION_JSON).build();
//        }
//        return Response.status(Response.Status.BAD_REQUEST)
//                .entity(Collections.singletonMap("error", "Unsupported command"))
//                .type(MediaType.APPLICATION_JSON)
//                .build();
//    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getTable(String warehouse, String db, String schema, String table)
    {
        Object wh = dataStore.get(warehouse);
        if (!(wh instanceof Map)) throw new IllegalArgumentException("Warehouse not found");

        Object dbMap = ((Map<String, Object>) wh).get(db);
        if (!(dbMap instanceof Map)) throw new IllegalArgumentException("Database not found");

        Object schemaMap = ((Map<String, Object>) dbMap).get(schema);
        if (!(schemaMap instanceof Map)) throw new IllegalArgumentException("Schema not found");

        Object tbl = ((Map<String, Object>) schemaMap).get(table);
        if (!(tbl instanceof List)) throw new IllegalArgumentException("Table not found");

        return (List<Map<String, Object>>) tbl;
    }
    private boolean isTypeMatch(Object value, String type)
    {
        if (value == null)
        {
            return false;
        }

        String lowerType = type.toLowerCase();

        if (lowerType.equals("int") || lowerType.equals("integer"))
        {
            return value instanceof Integer || (value instanceof Number && ((Number) value).intValue() == ((Number) value).doubleValue());
        }
        else if (lowerType.equals("float") || lowerType.equals("double"))
        {
            return value instanceof Float || value instanceof Double || value instanceof Number;
        }
        else if (lowerType.equals("string"))
        {
            return value instanceof String;
        }
        else if (lowerType.equals("boolean"))
        {
            return value instanceof Boolean;
        }
        else
        {
            return false;
        }
    }
}
