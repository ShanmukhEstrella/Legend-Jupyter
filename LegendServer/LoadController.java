package org.finos.legend.pylegend;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.*;
import java.util.stream.Collectors;


@Path("/data")
public class LoadController
{
    // Simulated in-memory database
    private static final Map<String, Object> dataStore = new HashMap<>();

    @POST
    @Path("/createtable")
    @Consumes(MediaType.TEXT_PLAIN)
    @Produces(MediaType.APPLICATION_JSON)
    public Response loadTable(String payload) {
        if (!payload.startsWith("create ")) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Payload must start with 'create '")).build();
        }

        String rest = payload.substring(6).trim();

        int firstParen = rest.indexOf('(');
        int lastParen = rest.lastIndexOf(')');

        if (firstParen == -1 || lastParen == -1 || lastParen <= firstParen) {
            return Response.status(Response.Status.BAD_REQUEST)
                    .entity(Map.of("error", "Missing or malformed schema declaration")).build();
        }

        String pathPart = rest.substring(0, firstParen).trim();
        String schemaPart = rest.substring(firstParen + 1, lastParen).trim();

        String[] parts = pathPart.split("::");
        if (parts.length != 4) {
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

    @GET
    @Path("/get")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTableData(@QueryParam("warehouse") String warehouse,
                                 @QueryParam("database") String db,
                                 @QueryParam("schema") String schema,
                                 @QueryParam("table") String table)
    {
        try
        {
            List<Map<String, Object>> tableData = getTable(warehouse, db, schema, table);
            return Response.ok(tableData).build();
        }
        catch (Exception e)
        {
            return Response.status(Response.Status.NOT_FOUND)
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
